"""
Placement Heuristics - Decides whether fields go to SQL or MongoDB
Based on frequency, type stability, and data characteristics
"""
from typing import Dict, Any, List
from metadata_store import MetadataStore
from type_detector import TypeDetector


class PlacementHeuristics:
    """
    Enhanced decision engine for determining optimal storage backend (SQL vs MongoDB).
    
    Improvements over basic heuristics:
    1. Soft Thresholds with Zones - Handles edge cases with zone-based logic
    2. Confidence Scoring - Combines multiple signals with explainability
    3. Gradual Drift Response - Prevents premature SQL exodus
    4. Booster Signals - Promotes high-quality fields that miss hard thresholds
    
    Core Heuristics:
    1. Frequency: Fields appearing frequently -> SQL
    2. Type Stability: Fields with consistent types -> SQL
    3. Atomicity: Nested/array fields -> MongoDB
    4. Mandatory: username, sys_ingested_at, t_stamp -> BOTH
    """
    
    # Zone-based thresholds (soft boundaries)
    FREQUENCY_ZONES = {
        'high': (0.75, 1.0),      # 75-100%: Definitely SQL
        'medium': (0.50, 0.75),   # 50-75%: Depends on other factors
        'low': (0.0, 0.50)        # 0-50%: Lean MongoDB
    }
    
    STABILITY_ZONES = {
        'stable': (0.85, 1.0),    # 85-100%: Type stable
        'moderate': (0.70, 0.85), # 70-85%: Moderately stable
        'unstable': (0.0, 0.70)   # 0-70%: Type unstable
    }
    
    # Legacy thresholds (maintained for compatibility)
    FREQUENCY_THRESHOLD = 60.0  # Percentage
    TYPE_STABILITY_THRESHOLD = 80.0  # Percentage
    MIN_OBSERVATIONS = 10  # Minimum records before making placement decision
    
    # Confidence and drift thresholds
    CONFIDENCE_THRESHOLD = 0.65
    MINOR_DRIFT_THRESHOLD = 0.10
    MODERATE_DRIFT_THRESHOLD = 0.25
    
    # Booster thresholds
    NULL_RATIO_THRESHOLD = 0.05  # Less than 5% nulls
    BOOSTER_PROMOTION_COUNT = 2  # Need at least 2 boosters
    RELAXED_FREQUENCY = 50.0     # Relaxed threshold with boosters
    RELAXED_STABILITY = 75.0     # Relaxed threshold with boosters
    
    def __init__(self, metadata_store: MetadataStore):
        self.metadata_store = metadata_store
        self.type_detector = TypeDetector()
    
    def decide_placement(self, normalized_key: str) -> str:
        """
        Enhanced placement decision with soft thresholds, confidence scoring,
        and booster signals.
        
        Decision Logic (Priority Order):
        1. Mandatory fields (username, timestamps) -> Both
        2. Structural checks (nested/array) -> MongoDB
        3. Zone-based classification with confidence scoring
        4. Booster signal promotion (semantic, uniqueness, consistency)
        5. Drift-based degradation (gradual response)
        
        Returns: 'SQL', 'MongoDB', or 'Both'
        """
        
        # Check if already decided
        existing_decision = self.metadata_store.get_placement_decision(normalized_key)
        if existing_decision:
            return existing_decision['backend']
        
        # Step 1: Mandatory fields go to BOTH backends for traceability
        mandatory_fields = {'username', 'sys_ingested_at', 't_stamp'}
        if normalized_key in mandatory_fields:
            reason = f"Mandatory field '{normalized_key}' stored in both backends for join capability"
            self.metadata_store.set_placement_decision(normalized_key, 'Both', reason)
            return 'Both'
        
        # Get field statistics
        field_data = self.metadata_store.metadata['fields'].get(normalized_key)
        if not field_data:
            # New field - defer decision
            return 'MongoDB'  # Default to MongoDB for unknown fields
        
        # Check if we have enough observations
        if field_data['appearances'] < self.MIN_OBSERVATIONS:
            # Not enough data - defer to MongoDB (flexible schema)
            return 'MongoDB'
        
        # Step 2: Get comprehensive field statistics
        stats = self.metadata_store.get_field_stats(normalized_key)
        frequency = stats['frequency']
        stability = stats['type_stability']
        drift_score = stats['drift_score']
        dominant_type = stats['dominant_type']
        
        # Step 3: Structural checks - nested objects or arrays -> MongoDB
        if dominant_type in ['dict', 'list']:
            reason = (f"Field '{normalized_key}' contains nested structures "
                     f"(type: {dominant_type}) - better suited for MongoDB")
            self.metadata_store.set_placement_decision(normalized_key, 'MongoDB', reason)
            return 'MongoDB'
        
        # Step 4: Zone classification
        freq_zone = self._get_zone(frequency, self.FREQUENCY_ZONES)
        stab_zone = self._get_zone(stability, self.STABILITY_ZONES)
        
        # Step 5: Calculate confidence score
        confidence = self._calculate_confidence(stats)
        
        # Step 6: Count booster signals
        booster_count = self._count_boosters(normalized_key, stats)
        
        # Step 7: Make placement decision using zones and confidence
        decision = self._decide_with_zones_and_confidence(
            freq_zone, stab_zone, confidence, booster_count, 
            frequency, stability
        )
        
        # Step 8: Apply drift response (can override decision)
        if drift_score >= self.MINOR_DRIFT_THRESHOLD:
            decision = self._handle_type_drift(normalized_key, drift_score, decision)
        
        # Step 9: Store decision with detailed reasoning
        reason = self._generate_reasoning(
            normalized_key, freq_zone, stab_zone, confidence, 
            booster_count, drift_score, frequency, stability, decision
        )
        self.metadata_store.set_placement_decision(normalized_key, decision, reason)
        
        return decision
    
    def _get_zone(self, value: float, zones: Dict[str, tuple]) -> str:
        """
        Classify a value into a zone (high/medium/low or stable/moderate/unstable).
        
        Args:
            value: The value to classify (e.g., frequency or stability percentage)
            zones: Dictionary mapping zone names to (min, max) tuples
        
        Returns:
            Zone name as string
        """
        for zone_name, (low, high) in zones.items():
            if low <= value / 100.0 <= high:  # Convert percentage to 0-1 range
                return zone_name
        return 'low'  # Default fallback
    
    def _calculate_confidence(self, stats: Dict[str, Any]) -> float:
        """
        Calculate confidence score for SQL placement.
        
        Combines multiple signals:
        - Frequency confidence (normalized to 0-1)
        - Stability confidence (normalized to 0-1)
        - Semantic confidence (0 or 0.8 based on pattern detection)
        
        Returns: Average confidence score (0-1 range)
        """
        # Normalize frequency to confidence (80% frequency = 1.0 confidence)
        freq_confidence = min(stats['frequency'] / 80.0, 1.0)
        
        # Normalize stability to confidence
        stab_confidence = stats['type_stability'] / 100.0
        
        # Semantic pattern boost
        semantic_confidence = 0.0
        if self._is_semantic_type(stats['dominant_type']):
            semantic_confidence = 0.8
        
        # Simple average (not weighted for transparency)
        confidence = (freq_confidence + stab_confidence + semantic_confidence) / 3.0
        
        return confidence
    
    def _count_boosters(self, normalized_key: str, stats: Dict[str, Any]) -> int:
        """
        Count booster signals that can promote a field to SQL.
        
        Boosters:
        1. Semantic type (IP, email, UUID, timestamp patterns)
        2. High uniqueness (likely identifier)
        3. Consistent non-null values (< 5% nulls)
        
        Returns: Number of boosters (0-3)
        """
        boosters = 0
        
        # Booster 1: Semantic type detection
        if self._is_semantic_type(stats['dominant_type']):
            boosters += 1
        
        # Booster 2: High uniqueness (identifier-like)
        if self.should_be_unique(normalized_key):
            boosters += 1
        
        # Booster 3: Consistent non-null values
        if stats.get('null_ratio', 1.0) < self.NULL_RATIO_THRESHOLD:
            boosters += 1
        
        return boosters
    
    def _decide_with_zones_and_confidence(
        self, freq_zone: str, stab_zone: str, confidence: float,
        booster_count: int, frequency: float, stability: float
    ) -> str:
        """
        Make placement decision using zone-based logic and confidence scoring.
        
        Decision hierarchy:
        1. High frequency + stable/moderate stability -> SQL
        2. Medium frequency + stable + good confidence -> SQL
        3. Booster promotion (2+ boosters + relaxed thresholds) -> SQL
        4. Default -> MongoDB
        """
        # Rule 1: High frequency + stable or moderate stability
        if freq_zone == 'high' and stab_zone in ['stable', 'moderate']:
            return 'SQL'
        
        # Rule 2: Medium frequency + stable + sufficient confidence
        if freq_zone == 'medium' and stab_zone == 'stable' and confidence >= 0.60:
            return 'SQL'
        
        # Rule 3: Booster promotion (relaxed thresholds)
        if booster_count >= self.BOOSTER_PROMOTION_COUNT:
            if freq_zone != 'low' and confidence >= 0.55:
                # Additional check: must meet relaxed thresholds
                if frequency >= self.RELAXED_FREQUENCY and stability >= self.RELAXED_STABILITY:
                    return 'SQL'
        
        # Default: MongoDB for safety and flexibility
        return 'MongoDB'
    
    def _handle_type_drift(self, normalized_key: str, drift_score: float, 
                          current_decision: str) -> str:
        """
        Graduated response to type drift instead of immediate quarantine.
        
        Drift levels:
        - Minor (0.10-0.25): Downgrade SQL to MongoDB, keep MongoDB
        - Severe (>0.25): Force MongoDB with quarantine flag
        
        Args:
            normalized_key: Field name
            drift_score: Drift score (0-1 range, higher = more drift)
            current_decision: Current placement decision
        
        Returns: Updated placement decision
        """
        if drift_score < self.MODERATE_DRIFT_THRESHOLD:
            # Minor to moderate drift (0.10 - 0.25)
            if current_decision == 'SQL':
                # Downgrade from SQL to MongoDB
                print(f"[PlacementHeuristics] Moderate drift detected for '{normalized_key}' "
                      f"({drift_score:.2f}), downgrading SQL -> MongoDB")
                return 'MongoDB'
            # If already MongoDB, keep it
            return 'MongoDB'
        else:
            # Severe drift (>= 0.25)
            print(f"[PlacementHeuristics] Severe drift detected for '{normalized_key}' "
                  f"({drift_score:.2f}), quarantining to MongoDB")
            self.metadata_store.mark_quarantined(normalized_key, drift_score)
            return 'MongoDB'
    
    def _is_semantic_type(self, field_type: str) -> bool:
        """
        Check if a field type matches semantic patterns.
        
        Semantic types: uuid, email, ip_address, timestamp, url, phone
        """
        semantic_types = {
            'uuid', 'email', 'ip_address', 'timestamp', 
            'url', 'phone', 'datetime', 'date'
        }
        return field_type in semantic_types
    
    def _generate_reasoning(
        self, normalized_key: str, freq_zone: str, stab_zone: str,
        confidence: float, booster_count: int, drift_score: float,
        frequency: float, stability: float, decision: str
    ) -> str:
        """
        Generate detailed reasoning for placement decision.
        
        Returns: Human-readable explanation string
        """
        reasons = []
        
        # Add zone information
        reasons.append(f"freq_zone={freq_zone}({frequency:.1f}%)")
        reasons.append(f"stab_zone={stab_zone}({stability:.1f}%)")
        reasons.append(f"confidence={confidence:.2f}")
        
        # Add booster information
        if booster_count > 0:
            reasons.append(f"boosters={booster_count}")
        
        # Add drift information
        if drift_score >= self.MINOR_DRIFT_THRESHOLD:
            reasons.append(f"drift={drift_score:.2f}")
        
        # Main reason based on decision
        if decision == 'SQL':
            if freq_zone == 'high':
                reason_text = f"High frequency + {stab_zone} stability"
            elif booster_count >= 2:
                reason_text = f"Promoted by {booster_count} boosters"
            else:
                reason_text = f"Medium frequency with stable types"
        else:
            if drift_score >= self.MODERATE_DRIFT_THRESHOLD:
                reason_text = f"Severe drift quarantine"
            elif drift_score >= self.MINOR_DRIFT_THRESHOLD:
                reason_text = f"Type drift detected"
            elif freq_zone == 'low':
                reason_text = f"Low frequency"
            else:
                reason_text = f"Insufficient confidence for SQL"
        
        return f"{reason_text} [{', '.join(reasons)}]"
    
    def should_be_unique(self, normalized_key: str) -> bool:
        """
        Determine if a field should have a UNIQUE constraint in SQL.
        
        Criteria:
        - Field name suggests uniqueness (id, uuid, session_id)
        - High cardinality in observed values
        - Consistent type (UUID or integer)
        """
        field_data = self.metadata_store.metadata['fields'].get(normalized_key)
        if not field_data:
            return False
        
        # Name-based heuristic (use word boundaries to avoid false matches like 'humidity')
        import re
        unique_indicators = [r'\bid\b', r'\buuid\b', r'\bsession\b', r'\bkey\b']
        field_lower = normalized_key.lower()
        has_unique_name = any(re.search(pattern, field_lower) for pattern in unique_indicators)
        
        # Exclude username from unique constraint (it's from a pool of 1000)
        if normalized_key == 'username':
            return False
        
        # Type-based heuristic
        dominant_type, _ = self.metadata_store.get_field_type_stability(normalized_key)
        unique_types = {'uuid', 'integer'}
        has_unique_type = dominant_type in unique_types
        
        # Sample cardinality check
        sample_values = field_data.get('sample_values', [])
        if len(sample_values) > 1:
            unique_ratio = len(set(sample_values)) / len(sample_values)
            has_high_cardinality = unique_ratio > 0.9
        else:
            has_high_cardinality = False
        
        # Decide: need at least name indicator + one other factor
        return has_unique_name and (has_unique_type or has_high_cardinality)
    
    def should_be_indexed(self, normalized_key: str) -> bool:
        """
        Determine if a field should be indexed.
        
        Criteria:
        - High frequency (!>50%)
        - Used in queries (username, timestamps, ids)
        """
        frequency = self.metadata_store.get_field_frequency(normalized_key)
        
        # High frequency fields
        if frequency >= 50.0:
            return True
        
        # Query-likely fields
        query_fields = ['username', 'timestamp', 't_stamp', 'sys_ingested_at', 
                       'session_id', 'device_id', 'user_id']
        if normalized_key in query_fields:
            return True
        
        return False
    
    def get_placement_summary(self) -> Dict[str, Any]:
        """
        Get summary of placement decisions for reporting.
        """
        sql_fields = []
        mongo_fields = []
        both_fields = []
        
        for field_name, decision in self.metadata_store.metadata['placement_decisions'].items():
            backend = decision['backend']
            if backend == 'SQL':
                sql_fields.append(field_name)
            elif backend == 'MongoDB':
                mongo_fields.append(field_name)
            elif backend == 'Both':
                both_fields.append(field_name)
        
        return {
            'sql_count': len(sql_fields),
            'mongodb_count': len(mongo_fields),
            'both_count': len(both_fields),
            'sql_fields': sql_fields,
            'mongodb_fields': mongo_fields,
            'both_fields': both_fields,
            'thresholds': {
                'frequency_zones': self.FREQUENCY_ZONES,
                'stability_zones': self.STABILITY_ZONES,
                'confidence_threshold': self.CONFIDENCE_THRESHOLD,
                'drift_thresholds': {
                    'minor': self.MINOR_DRIFT_THRESHOLD,
                    'moderate': self.MODERATE_DRIFT_THRESHOLD
                },
                'booster_promotion_count': self.BOOSTER_PROMOTION_COUNT,
                'legacy': {
                    'frequency_threshold': self.FREQUENCY_THRESHOLD,
                    'type_stability_threshold': self.TYPE_STABILITY_THRESHOLD,
                    'min_observations': self.MIN_OBSERVATIONS
                }
            }
        }


# Example usage
if __name__ == "__main__":
    from metadata_store import MetadataStore
    
    # Create test scenario
    store = MetadataStore('test_metadata.json')
    heuristics = PlacementHeuristics(store)
    
    # Simulate some field tracking
    store.metadata['total_records'] = 100
    
    # High frequency, stable field
    store.metadata['fields']['email'] = {
        'appearances': 95,
        'type_counts': {'email': 95},
        'sample_values': ['user1@test.com', 'user2@test.com']
    }
    
    # Low frequency field
    store.metadata['fields']['altitude'] = {
        'appearances': 30,
        'type_counts': {'float': 30},
        'sample_values': [100.5, 200.3, 150.0]
    }
    
    # Type drifting field
    store.metadata['fields']['battery'] = {
        'appearances': 80,
        'type_counts': {'integer': 50, 'string': 30},
        'sample_values': ['50', 60, '70', 80]
    }
    
    # Test decisions
    print("Placement Decisions:")
    print("-" * 60)
    print(f"email: {heuristics.decide_placement('email')}")
    print(f"altitude: {heuristics.decide_placement('altitude')}")
    print(f"battery: {heuristics.decide_placement('battery')}")
    print(f"username: {heuristics.decide_placement('username')}")
    
    print("\nSummary:")
    print(heuristics.get_placement_summary())

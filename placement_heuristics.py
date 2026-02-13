"""
Placement Heuristics - Decides whether fields go to SQL or MongoDB
Based on frequency, type stability, and data characteristics
"""
from typing import Dict, Any, List
from metadata_store import MetadataStore
from type_detector import TypeDetector


class PlacementHeuristics:
    """
    Decision engine for determining optimal storage backend (SQL vs MongoDB).
    
    Heuristics:
    1. Frequency: Fields appearing in >60% of records -> SQL
    2. Type Stability: Fields with >80% type consistency -> SQL
    3. Atomicity: Nested/array fields -> MongoDB
    4. Mandatory: username, sys_ingested_at, t_stamp -> BOTH
    """
    
    # Thresholds (adjustable based on requirements)
    FREQUENCY_THRESHOLD = 60.0  # Percentage
    TYPE_STABILITY_THRESHOLD = 80.0  # Percentage
    MIN_OBSERVATIONS = 10  # Minimum records before making placement decision
    
    def __init__(self, metadata_store: MetadataStore):
        self.metadata_store = metadata_store
        self.type_detector = TypeDetector()
    
    def decide_placement(self, normalized_key: str) -> str:
        """
        Decide where a field should be stored: 'SQL', 'MongoDB', or 'Both'
        
        Decision Logic:
        1. Mandatory fields (username, timestamps) -> Both
        2. Nested/array fields -> MongoDB
        3. High frequency (>60%) + High type stability (>80%) -> SQL
        4. Low frequency (<60%) OR type drifting -> MongoDB
        5. Fields with atomic types but inconsistent appearance -> MongoDB
        
        Returns: 'SQL', 'MongoDB', or 'Both'
        """
        
        # Check if already decided
        existing_decision = self.metadata_store.get_placement_decision(normalized_key)
        if existing_decision:
            return existing_decision['backend']
        
        # Rule 1: Mandatory fields go to BOTH backends for traceability
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
        
        # Get frequency and type stability
        frequency = self.metadata_store.get_field_frequency(normalized_key)
        dominant_type, type_stability = self.metadata_store.get_field_type_stability(normalized_key)
        
        # Rule 2: Nested objects or arrays -> MongoDB
        if dominant_type in ['dict', 'list']:
            reason = (f"Field '{normalized_key}' contains nested structures "
                     f"(type: {dominant_type}) - better suited for MongoDB")
            self.metadata_store.set_placement_decision(normalized_key, 'MongoDB', reason)
            return 'MongoDB'
        
        # Rule 3: High frequency + High type stability -> SQL
        if frequency >= self.FREQUENCY_THRESHOLD and type_stability >= self.TYPE_STABILITY_THRESHOLD:
            reason = (f"Field '{normalized_key}' has high frequency ({frequency:.1f}%) "
                     f"and type stability ({type_stability:.1f}%) - optimal for SQL")
            self.metadata_store.set_placement_decision(normalized_key, 'SQL', reason)
            return 'SQL'
        
        # Rule 4: Low frequency -> MongoDB
        if frequency < self.FREQUENCY_THRESHOLD:
            reason = (f"Field '{normalized_key}' has low frequency ({frequency:.1f}%) "
                     f"- storing in MongoDB for sparse data flexibility")
            self.metadata_store.set_placement_decision(normalized_key, 'MongoDB', reason)
            return 'MongoDB'
        
        # Rule 5: Type drifting -> MongoDB
        if type_stability < self.TYPE_STABILITY_THRESHOLD:
            reason = (f"Field '{normalized_key}' has unstable types ({type_stability:.1f}% stability) "
                     f"- MongoDB can handle schema flexibility")
            self.metadata_store.set_placement_decision(normalized_key, 'MongoDB', reason)
            return 'MongoDB'
        
        # Default: MongoDB for safety
        reason = f"Field '{normalized_key}' defaulted to MongoDB for flexibility"
        self.metadata_store.set_placement_decision(normalized_key, 'MongoDB', reason)
        return 'MongoDB'
    
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
                'frequency_threshold': self.FREQUENCY_THRESHOLD,
                'type_stability_threshold': self.TYPE_STABILITY_THRESHOLD,
                'min_observations': self.MIN_OBSERVATIONS
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

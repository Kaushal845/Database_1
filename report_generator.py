"""
Report Generator - Answers mandatory assignment questions
Provides comprehensive technical analysis of the ingestion system
"""
import json
from datetime import datetime
from typing import Dict, Any
from metadata_store import MetadataStore
from placement_heuristics import PlacementHeuristics
from database_managers import SQLManager, MongoDBManager


class ReportGenerator:
    """
    Generates a comprehensive technical report answering all mandatory questions.
    """
    
    def __init__(self, metadata_store: MetadataStore):
        self.metadata_store = metadata_store
        self.placement_heuristics = PlacementHeuristics(metadata_store)
    
    def generate_full_report(self, output_file='TECHNICAL_REPORT.md') -> str:
        """
        Generate the complete technical report and save to file.
        """
        report = []
        
        # Header
        report.append("# AUTONOMOUS DATA INGESTION SYSTEM - TECHNICAL REPORT")
        report.append(f"\n**Generated:** {datetime.utcnow().isoformat()}")
        report.append(f"\n**System Status:** Active")
        report.append(f"\n**Total Records Processed:** {self.metadata_store.metadata['total_records']}")
        report.append("\n" + "=" * 80 + "\n")
        
        # Question 1: Normalization Strategy
        report.append(self._answer_question_1())
        
        # Question 2: Placement Heuristics
        report.append(self._answer_question_2())
        
        # Question 3: Uniqueness Strategy
        report.append(self._answer_question_3())
        
        # Question 4: Value Interpretation
        report.append(self._answer_question_4())
        
        # Question 5: Mixed Data Handling
        report.append(self._answer_question_5())
        
        # Additional Analysis
        report.append(self._generate_statistics())
        
        # Field-by-Field Analysis
        report.append(self._generate_field_analysis())
        
        # System Architecture
        report.append(self._describe_architecture())
        
        # Conclusion
        report.append(self._generate_conclusion())
        
        # Combine and save
        full_report = "\n".join(report)
        
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(full_report)
        
        print(f"[Report] Generated: {output_file}")
        return full_report
    
    def _answer_question_1(self) -> str:
        """
        Question 1: How did you resolve type naming ambiguities?
        """
        section = []
        section.append("## 1. Normalization Strategy")
        section.append("\n### Problem Statement")
        section.append("Fields may appear with different casings (e.g., `ip`, `IP`, `IpAddress`) or naming")
        section.append("styles (`user_name` vs `userName`). Without normalization, these would create duplicate")
        section.append("columns in SQL, wasting storage and causing query complexity.")
        
        section.append("\n### Our Solution: Multi-Stage Normalization")
        
        section.append("\n#### Stage 1: Syntax Normalization")
        section.append("1. **CamelCase to snake_case conversion**")
        section.append("   - `userName` → `user_name`")
        section.append("   - `IpAddress` → `ip_address`")
        section.append("   - Uses regex pattern matching: `(.)([A-Z][a-z]+)` → `\\1_\\2`")
        
        section.append("\n2. **Lowercase conversion**")
        section.append("   - All field names converted to lowercase")
        section.append("   - `IP` → `ip`, `Email` → `email`")
        
        section.append("\n3. **Underscore cleanup**")
        section.append("   - Multiple underscores collapsed: `user__name` → `user_name`")
        section.append("   - Leading/trailing underscores removed")
        
        section.append("\n#### Stage 2: Semantic Normalization")
        section.append("Applied pattern-based rules to map semantically equivalent fields:")
        section.append("\n```")
        section.append("IP Patterns:     ip, IP, ipAddress, ip_address → ip_address")
        section.append("User Patterns:   userName, user_name, username → username")
        section.append("Email Patterns:  email, eMail, emailAddress → email")
        section.append("Time Patterns:   timestamp, timeStamp, t_stamp → timestamp")
        section.append("GPS Patterns:    lat, latitude, gps_lat → gps_lat")
        section.append("```")
        
        section.append("\n#### Implementation Details")
        section.append("- **Module:** `field_normalizer.py`")
        section.append("- **Method:** `FieldNormalizer.normalize(field_name)`")
        section.append("- **Storage:** Normalization mappings persisted in `metadata_store.json`")
        section.append("  under `normalization_rules` for consistency across restarts")
        
        section.append("\n### Observed Normalizations")
        if self.metadata_store.metadata['normalization_rules']:
            section.append("\n| Original Key | Normalized Key |")
            section.append("|--------------|----------------|")
            for original, normalized in list(self.metadata_store.metadata['normalization_rules'].items())[:15]:
                section.append(f"| {original} | {normalized} |")
            
            if len(self.metadata_store.metadata['normalization_rules']) > 15:
                remaining = len(self.metadata_store.metadata['normalization_rules']) - 15
                section.append(f"| ... | *({remaining} more mappings)* |")
        else:
            section.append("\n*No ambiguous field names encountered yet.*")
        
        section.append("\n### Rules Enforcement")
        section.append("1. **First occurrence wins**: When a field is first seen, its normalized form is recorded")
        section.append("2. **Consistent mapping**: All future occurrences of variants map to the same normalized key")
        section.append("3. **Persistent storage**: Mappings survive system restarts via metadata store")
        section.append("4. **Case-insensitive matching**: `IP` and `ip` are treated identically")
        
        return "\n".join(section) + "\n\n"
    
    def _answer_question_2(self) -> str:
        """
        Question 2: What specific thresholds were used for placement decisions?
        """
        section = []
        section.append("## 2. Placement Heuristics: SQL vs MongoDB")
        
        section.append("\n### Decision Framework")
        section.append("Our system uses a **multi-criteria decision engine** with adaptive thresholds:")
        
        section.append("\n### Thresholds Configuration")
        section.append(f"- **Frequency Threshold:** {self.placement_heuristics.FREQUENCY_THRESHOLD}%")
        section.append(f"- **Type Stability Threshold:** {self.placement_heuristics.TYPE_STABILITY_THRESHOLD}%")
        section.append(f"- **Minimum Observations:** {self.placement_heuristics.MIN_OBSERVATIONS} records")
        
        section.append("\n### Placement Rules (in priority order)")
        
        section.append("\n#### Rule 1: Mandatory Fields → BOTH")
        section.append("Fields: `username`, `sys_ingested_at`, `t_stamp`")
        section.append("- **Rationale:** These fields enable joining data across SQL and MongoDB")
        section.append("- **Use case:** Query SQL for structured data, then fetch full document from MongoDB")
        section.append("  using `sys_ingested_at` as the join key")
        
        section.append("\n#### Rule 2: Nested/Array Fields → MongoDB")
        section.append("Condition: `type == 'dict' OR type == 'list'`")
        section.append("- **Rationale:** SQL requires flattening or JSON serialization (lossy)")
        section.append("- **MongoDB advantage:** Native support for nested documents and arrays")
        section.append("- **Example:** `metadata: {sensor: {version: '2.1'}}`")
        
        section.append(f"\n#### Rule 3: High Frequency + Stable Type → SQL")
        section.append(f"Conditions:")
        section.append(f"- Frequency ≥ {self.placement_heuristics.FREQUENCY_THRESHOLD}%")
        section.append(f"- Type Stability ≥ {self.placement_heuristics.TYPE_STABILITY_THRESHOLD}%")
        section.append("- **Rationale:** Consistent, frequently-appearing fields benefit from SQL's schema")
        section.append("- **SQL advantages:**")
        section.append("  - Efficient storage (fixed schema)")
        section.append("  - Fast queries with indexes")
        section.append("  - Type constraints prevent errors")
        section.append("- **Example:** `email` appears in 95% of records, always as string")
        
        section.append(f"\n#### Rule 4: Low Frequency → MongoDB")
        section.append(f"Condition: Frequency < {self.placement_heuristics.FREQUENCY_THRESHOLD}%")
        section.append("- **Rationale:** Sparse fields waste space in SQL (many NULL values)")
        section.append("- **MongoDB advantage:** Fields only consume space when present")
        section.append("- **Example:** `altitude` appears in only 30% of records")
        
        section.append(f"\n#### Rule 5: Type Drifting → MongoDB")
        section.append(f"Condition: Type Stability < {self.placement_heuristics.TYPE_STABILITY_THRESHOLD}%")
        section.append("- **Rationale:** SQL schemas expect consistent types")
        section.append("- **MongoDB advantage:** Flexible schema handles type variations")
        section.append("- **Example:** `battery` sometimes integer (50), sometimes string ('50%')")
        
        section.append("\n### Placement Distribution")
        placement_summary = self.placement_heuristics.get_placement_summary()
        section.append(f"\n- **SQL Only:** {placement_summary['sql_count']} fields")
        section.append(f"- **MongoDB Only:** {placement_summary['mongodb_count']} fields")
        section.append(f"- **Both:** {placement_summary['both_count']} fields")
        
        section.append("\n### Example Placements")
        if placement_summary['sql_fields']:
            section.append("\n**SQL Fields:** " + ", ".join([f"`{f}`" for f in placement_summary['sql_fields'][:10]]))
        if placement_summary['mongodb_fields']:
            section.append("\n**MongoDB Fields:** " + ", ".join([f"`{f}`" for f in placement_summary['mongodb_fields'][:10]]))
        if placement_summary['both_fields']:
            section.append("\n**Both Backends:** " + ", ".join([f"`{f}`" for f in placement_summary['both_fields']]))
        
        return "\n".join(section) + "\n\n"
    
    def _answer_question_3(self) -> str:
        """
        Question 3: How did you identify which fields should be UNIQUE?
        """
        section = []
        section.append("## 3. Uniqueness Detection Strategy")
        
        section.append("\n### Problem Statement")
        section.append("Not all frequent fields should be UNIQUE. For example, `username` is frequent but")
        section.append("non-unique (from a pool of 1000 users). We need intelligent heuristics to identify")
        section.append("true unique identifiers.")
        
        section.append("\n### Our Multi-Factor Approach")
        
        section.append("\n#### Factor 1: Name-Based Heuristics")
        section.append("Field names containing these keywords suggest uniqueness:")
        section.append("- `id` (e.g., `device_id`, `session_id`, `user_id`)")
        section.append("- `uuid`")
        section.append("- `key`")
        section.append("- `session`")
        section.append("\n**Rationale:** Naming conventions often signal intent")
        
        section.append("\n#### Factor 2: Type-Based Heuristics")
        section.append("Fields with these semantic types suggest uniqueness:")
        section.append("- `uuid` (e.g., '550e8400-e29b-41d4-a716-446655440000')")
        section.append("- `integer` (when combined with name heuristic)")
        section.append("\n**Rationale:** UUIDs are designed for uniqueness; integer IDs are common patterns")
        
        section.append("\n#### Factor 3: Cardinality Analysis")
        section.append("We track sample values and compute:")
        section.append("```")
        section.append("Unique Ratio = Number of Unique Values / Total Values")
        section.append("```")
        section.append("- If Unique Ratio > 0.9 (90%), cardinality is high")
        section.append("\n**Rationale:** True unique fields have high diversity")
        
        section.append("\n### Decision Logic")
        section.append("A field is marked UNIQUE if:")
        section.append("```")
        section.append("Has Name Indicator (id/uuid/key)")
        section.append("  AND")
        section.append("(Has Unique Type OR High Cardinality)")
        section.append("```")
        
        section.append("\n### Special Cases")
        section.append("- **`username`**: Explicitly excluded (non-unique by design - 1000-user pool)")
        section.append("- **`sys_ingested_at`**: Marked UNIQUE (server-generated with microsecond precision)")
        section.append("- **Frequent non-IDs**: `email`, `phone` are frequent but not enforced as UNIQUE")
        section.append("  (may have duplicates in real-world data)")
        
        section.append("\n### Implementation")
        section.append("- **Module:** `placement_heuristics.py`")
        section.append("- **Method:** `PlacementHeuristics.should_be_unique(field_name)`")
        section.append("- **Application:** SQL schema uses `CREATE UNIQUE INDEX` for identified fields")
        
        # Find actual unique fields
        unique_fields = []
        for field_name in self.metadata_store.get_all_fields():
            if self.placement_heuristics.should_be_unique(field_name):
                unique_fields.append(field_name)
        
        if unique_fields:
            section.append("\n### Detected Unique Fields")
            for field in unique_fields:
                section.append(f"- `{field}`")
        
        return "\n".join(section) + "\n\n"
    
    def _answer_question_4(self) -> str:
        """
        Question 4: How did you differentiate between "1.2.3.4" (IP) and 1.2 (float)?
        """
        section = []
        section.append("## 4. Value Interpretation: Semantic Type Detection")
        
        section.append("\n### Challenge")
        section.append("Values can be ambiguous:")
        section.append("- `'1.2.3.4'` could be a string or an IP address")
        section.append("- `1.2` is clearly a float")
        section.append("- `'1.2'` is a string representing a number")
        section.append("\nNaive `type()` checking only reveals Python types, not semantic meaning.")
        
        section.append("\n### Our Solution: Cascading Type Detection")
        section.append("**Module:** `type_detector.py`")
        
        section.append("\n#### Detection Priority (checked in order)")
        
        section.append("\n**1. Null Check**")
        section.append("   - `value is None` → `'null'`")
        
        section.append("\n**2. Boolean Check**")
        section.append("   - `isinstance(value, bool)` → `'boolean'`")
        section.append("   - Must come before int check (bool is subclass of int in Python)")
        
        section.append("\n**3. Integer Check**")
        section.append("   - `isinstance(value, int)` → `'integer'`")
        
        section.append("\n**4. Float Check**")
        section.append("   - `isinstance(value, float)` → `'float'`")
        section.append("   - Example: `1.2` → `'float'`")
        
        section.append("\n**5. Collection Checks**")
        section.append("   - `isinstance(value, list)` → `'list'`")
        section.append("   - `isinstance(value, dict)` → `'dict'`")
        
        section.append("\n**6. String Semantic Analysis** (most complex)")
        section.append("   For string values, we apply regex patterns:")
        
        section.append("\n   **a) UUID Pattern**")
        section.append("   ```regex")
        section.append("   ^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")
        section.append("   ```")
        section.append("   Example: `'550e8400-e29b-41d4-a716-446655440000'` → `'uuid'`")
        
        section.append("\n   **b) IP Address Pattern** (CRITICAL for question 4)")
        section.append("   ```regex")
        section.append("   ^(\\d{1,3}\\.){3}\\d{1,3}$")
        section.append("   ```")
        section.append("   - Matches 4 dot-separated numeric parts")
        section.append("   - Additional validation: Each part must be 0-255")
        section.append("   - Example: `'1.2.3.4'` → `'ip_address'` (validated ✓)")
        section.append("   - Example: `'1.2'` → `'string'` (only 2 parts, fails pattern)")
        section.append("   - Example: `'999.1.1.1'` → `'string'` (999 > 255, invalid)")
        
        section.append("\n   **c) Email Pattern**")
        section.append("   ```regex")
        section.append("   ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$")
        section.append("   ```")
        section.append("   Example: `'user@example.com'` → `'email'`")
        
        section.append("\n   **d) URL Pattern**")
        section.append("   ```regex")
        section.append("   ^https?://[^\\s]+$")
        section.append("   ```")
        section.append("   Example: `'https://example.com'` → `'url'`")
        
        section.append("\n   **e) ISO Timestamp Pattern**")
        section.append("   ```regex")
        section.append("   ^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}")
        section.append("   ```")
        section.append("   Example: `'2024-01-15T10:30:00'` → `'timestamp'`")
        
        section.append("\n   **f) Fallback**")
        section.append("   - If no pattern matches → `'string'`")
        
        section.append("\n### Key Insight: Order Matters")
        section.append("IP detection happens **before** generic string classification, ensuring:")
        section.append("- `'1.2.3.4'` is recognized as `ip_address`, not `string`")
        section.append("- `1.2` is already caught by float check, never reaches string analysis")
        
        section.append("\n### Type Mapping to SQL")
        section.append("Once semantic type is detected, we map to SQL types:")
        section.append("```")
        section.append("boolean      → BOOLEAN")
        section.append("integer      → INTEGER")
        section.append("float        → REAL")
        section.append("ip_address   → VARCHAR(15)")
        section.append("uuid         → VARCHAR(36)")
        section.append("email        → VARCHAR(255)")
        section.append("url          → TEXT")
        section.append("timestamp    → TIMESTAMP")
        section.append("string       → TEXT")
        section.append("```")
        
        section.append("\n### Verification Example")
        section.append("```python")
        section.append("TypeDetector.detect_type('1.2.3.4')   # → 'ip_address'")
        section.append("TypeDetector.detect_type(1.2)         # → 'float'")
        section.append("TypeDetector.detect_type('1.2')       # → 'string' (only 2 parts)")
        section.append("TypeDetector.detect_type('192.168.1.1')  # → 'ip_address'")
        section.append("```")
        
        return "\n".join(section) + "\n\n"
    
    def _answer_question_5(self) -> str:
        """
        Question 5: How did your system react when a field's data type changed mid-stream?
        """
        section = []
        section.append("## 5. Mixed Data Handling: Type Drifting")
        
        section.append("\n### Problem Scenario")
        section.append("A field like `battery` might arrive as:")
        section.append("- Record 1: `50` (integer)")
        section.append("- Record 2: `'60%'` (string)")
        section.append("- Record 3: `75.5` (float)")
        section.append("- Record 4: `'charging'` (string)")
        section.append("\nSQL expects consistent types, but real-world data is messy.")
        
        section.append("\n### Our Adaptive Strategy")
        
        section.append("\n#### Phase 1: Detection & Tracking")
        section.append("For every field in every record:")
        section.append("1. Detect semantic type using `TypeDetector`")
        section.append("2. Increment type counter in metadata:")
        section.append("   ```python")
        section.append("   field_data['type_counts'][detected_type] += 1")
        section.append("   ```")
        section.append("3. Calculate type stability:")
        section.append("   ```python")
        section.append("   stability = (dominant_type_count / total_appearances) * 100")
        section.append("   ```")
        
        section.append("\n#### Phase 2: Dynamic Placement Adjustment")
        section.append(f"- If initial observations suggest SQL (high frequency, stable type)")
        section.append("  → Field starts going to SQL")
        section.append(f"- If type stability drops below {self.placement_heuristics.TYPE_STABILITY_THRESHOLD}%")
        section.append("  → **Placement decision updated to MongoDB**")
        section.append("  → Reason logged: 'Type drifting detected'")
        
        section.append("\n#### Phase 3: Coexistence Strategy")
        section.append("What happens to data already in SQL?")
        section.append("- **SQL Handling:**")
        section.append("  - Column type set to `TEXT` (most flexible)")
        section.append("  - All values stored as strings: `50`, `'60%'`, `'charging'`")
        section.append("  - No data loss, but type constraints relaxed")
        section.append("- **MongoDB Handling:**")
        section.append("  - New records with this field go to MongoDB")
        section.append("  - Native type preservation: integer stays integer, string stays string")
        section.append("  - No conversion needed")
        
        section.append("\n#### Phase 4: Metadata Persistence")
        section.append("All type tracking survives restarts:")
        section.append("```json")
        section.append("{")
        section.append("  'fields': {")
        section.append("    'battery': {")
        section.append("      'appearances': 100,")
        section.append("      'type_counts': {")
        section.append("        'integer': 50,")
        section.append("        'string': 40,")
        section.append("        'float': 10")
        section.append("      },")
        section.append("      'type_stability': 50.0,  // 50% integer")
        section.append("      'placement': 'MongoDB'  // Updated due to instability")
        section.append("    }")
        section.append("  }")
        section.append("}")
        section.append("```")
        
        section.append("\n### Real-World Impact")
        
        # Find fields with type drifting
        drifting_fields = []
        for field_name, field_data in self.metadata_store.metadata['fields'].items():
            if len(field_data.get('type_counts', {})) > 1:
                dominant_type, stability = self.metadata_store.get_field_type_stability(field_name)
                drifting_fields.append({
                    'field': field_name,
                    'types': field_data['type_counts'],
                    'dominant': dominant_type,
                    'stability': stability
                })
        
        if drifting_fields:
            section.append("\n### Detected Type Drifting Fields")
            section.append("\n| Field | Types Observed | Dominant Type | Stability |")
            section.append("|-------|----------------|---------------|-----------|")
            for field_info in drifting_fields[:10]:
                types_str = ", ".join([f"{t}({c})" for t, c in field_info['types'].items()])
                section.append(f"| `{field_info['field']}` | {types_str} | {field_info['dominant']} | {field_info['stability']:.1f}% |")
        else:
            section.append("\n*No type drifting detected in current dataset.*")
        
        section.append("\n### Advantages of Our Approach")
        section.append("1. **No data loss**: All records stored regardless of type inconsistency")
        section.append("2. **Graceful degradation**: SQL fields degrade to TEXT when needed")
        section.append("3. **Automatic adaptation**: System learns and adjusts without manual intervention")
        section.append("4. **Audit trail**: All type changes logged in metadata with timestamps")
        section.append("5. **Restart resilience**: Type history preserved across system restarts")
        
        return "\n".join(section) + "\n\n"
    
    def _generate_statistics(self) -> str:
        """Generate overall system statistics"""
        section = []
        section.append("## 6. System Statistics")
        
        stats = self.metadata_store.get_statistics()
        section.append(f"\n- **Total Records Ingested:** {stats['total_records']:,}")
        section.append(f"- **Unique Fields Discovered:** {stats['unique_fields']}")
        section.append(f"- **Normalization Rules Created:** {stats['normalization_rules']}")
        section.append(f"- **Placement Decisions Made:** {stats['placement_decisions']}")
        section.append(f"- **Session Start:** {stats['session_start']}")
        section.append(f"- **Last Updated:** {stats['last_updated']}")
        
        return "\n".join(section) + "\n\n"
    
    def _generate_field_analysis(self) -> str:
        """Generate detailed field-by-field analysis"""
        section = []
        section.append("## 7. Field-by-Field Analysis")
        
        section.append("\n| Field Name | Frequency | Type | Stability | Placement | Unique |")
        section.append("|------------|-----------|------|-----------|-----------|--------|")
        
        for field_name in sorted(self.metadata_store.get_all_fields()):
            frequency = self.metadata_store.get_field_frequency(field_name)
            dominant_type, stability = self.metadata_store.get_field_type_stability(field_name)
            placement_decision = self.metadata_store.get_placement_decision(field_name)
            placement = placement_decision['backend'] if placement_decision else 'Pending'
            is_unique = '✓' if self.placement_heuristics.should_be_unique(field_name) else ''
            
            section.append(f"| `{field_name}` | {frequency:.1f}% | {dominant_type} | {stability:.1f}% | {placement} | {is_unique} |")
        
        return "\n".join(section) + "\n\n"
    
    def _describe_architecture(self) -> str:
        """Describe system architecture"""
        section = []
        section.append("## 8. System Architecture")
        
        section.append("\n### Component Overview")
        section.append("\n```")
        section.append("┌─────────────────────────────────────────────────────────┐")
        section.append("│                    FastAPI Server                       │")
        section.append("│              (Synthetic Data Generator)                 │")
        section.append("└────────────────────┬────────────────────────────────────┘")
        section.append("                     │ JSON Stream")
        section.append("                     ▼")
        section.append("┌─────────────────────────────────────────────────────────┐")
        section.append("│                  Data Consumer                          │")
        section.append("│              (data_consumer.py)                        │")
        section.append("└────────────────────┬────────────────────────────────────┘")
        section.append("                     │ Raw Records")
        section.append("                     ▼")
        section.append("┌─────────────────────────────────────────────────────────┐")
        section.append("│              Ingestion Pipeline                         │")
        section.append("│           (ingestion_pipeline.py)                       │")
        section.append("│                                                         │")
        section.append("│  ┌──────────────────────────────────────────────────┐  │")
        section.append("│  │ 1. Field Normalizer (field_normalizer.py)       │  │")
        section.append("│  │    - Resolve naming ambiguities                  │  │")
        section.append("│  └──────────────────────────────────────────────────┘  │")
        section.append("│                     │")
        section.append("│  ┌──────────────────────────────────────────────────┐  │")
        section.append("│  │ 2. Type Detector (type_detector.py)              │  │")
        section.append("│  │    - Semantic type detection                     │  │")
        section.append("│  └──────────────────────────────────────────────────┘  │")
        section.append("│                     │")
        section.append("│  ┌──────────────────────────────────────────────────┐  │")
        section.append("│  │ 3. Metadata Store (metadata_store.py)            │  │")
        section.append("│  │    - Track frequency & type stability            │  │")
        section.append("│  └──────────────────────────────────────────────────┘  │")
        section.append("│                     │")
        section.append("│  ┌──────────────────────────────────────────────────┐  │")
        section.append("│  │ 4. Placement Heuristics (placement_heuristics.py)│  │")
        section.append("│  │    - Decide SQL vs MongoDB                       │  │")
        section.append("│  └──────────────────────────────────────────────────┘  │")
        section.append("│                     │")
        section.append("└─────────────────────┼───────────────────────────────────┘")
        section.append("                      │")
        section.append("        ┌─────────────┴──────────────┐")
        section.append("        ▼                            ▼")
        section.append(" ┌─────────────┐            ┌──────────────┐")
        section.append(" │ SQL Manager │            │ MongoDB Mgr  │")
        section.append(" │  (SQLite)   │            │   (MongoDB)  │")
        section.append(" └─────────────┘            └──────────────┘")
        section.append("```")
        
        section.append("\n### Key Files")
        section.append("- `metadata_store.py` - Persistent metadata storage")
        section.append("- `field_normalizer.py` - Field name normalization")
        section.append("- `type_detector.py` - Semantic type detection")
        section.append("- `placement_heuristics.py` - Placement decision logic")
        section.append("- `database_managers.py` - SQL & MongoDB interfaces")
        section.append("- `ingestion_pipeline.py` - Main orchestration")
        section.append("- `data_consumer.py` - API client")
        section.append("- `report_generator.py` - This report")
        
        return "\n".join(section) + "\n\n"
    
    def _generate_conclusion(self) -> str:
        """Generate conclusion"""
        section = []
        section.append("## 9. Conclusion")
        
        section.append("\n### Key Achievements")
        section.append("1. **Autonomous Operation**: No hardcoded field mappings")
        section.append("2. **Adaptive Learning**: System improves decisions as more data arrives")
        section.append("3. **Persistent Memory**: Survives restarts via metadata store")
        section.append("4. **Data Integrity**: Bi-temporal timestamps enable accurate joins")
        section.append("5. **Flexibility**: Handles unclean, heterogeneous data gracefully")
        
        section.append("\n### Design Principles")
        section.append("- **No Hardcoding**: All decisions data-driven")
        section.append("- **Gradual Learning**: Accumulate knowledge over time")
        section.append("- **Fail-Safe Defaults**: Unknown fields → MongoDB (flexible)")
        section.append("- **Traceability**: Username + sys_ingested_at maintained everywhere")
        
        section.append("\n### Future Enhancements")
        section.append("- Machine learning for placement optimization")
        section.append("- Automatic index creation based on query patterns")
        section.append("- Distributed processing for high-volume streams")
        section.append("- Real-time monitoring dashboard")
        
        section.append("\n---")
        section.append(f"\n*Report generated by `report_generator.py` on {datetime.utcnow().isoformat()}*")
        
        return "\n".join(section) + "\n"


# Main execution
if __name__ == "__main__":
    import sys
    
    # Load metadata store
    metadata_file = 'metadata_store.json'
    if len(sys.argv) > 1:
        metadata_file = sys.argv[1]
    
    print(f"[Report] Loading metadata from: {metadata_file}")
    
    try:
        metadata_store = MetadataStore(metadata_file)
        generator = ReportGenerator(metadata_store)
        
        # Generate report
        report = generator.generate_full_report()
        
        print("\n[Report] ✓ Report generated successfully!")
        print("[Report] File: TECHNICAL_REPORT.md")
        print(f"[Report] Size: {len(report)} characters")
        
    except FileNotFoundError:
        print(f"[Report] Error: {metadata_file} not found")
        print("[Report] Run the ingestion system first to generate metadata")
        sys.exit(1)

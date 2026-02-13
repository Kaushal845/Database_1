"""
Type Detector - Intelligently detect data types, including semantic types
Differentiates between "1.2.3.4" (IP string) and 1.2 (float)
"""
import re
from typing import Any, List
from datetime import datetime
import uuid


class TypeDetector:
    """
    Advanced type detection that goes beyond Python's built-in type().
    Identifies semantic types like IP addresses, UUIDs, timestamps, etc.
    """
    
    # Regex patterns for semantic type detection
    IP_PATTERN = re.compile(r'^(\d{1,3}\.){3}\d{1,3}$')
    UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
    EMAIL_PATTERN = re.compile(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')
    URL_PATTERN = re.compile(r'^https?://[^\s]+$')
    ISO_TIMESTAMP_PATTERN = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}')
    
    @staticmethod
    def detect_type(value: Any) -> str:
        """
        Detect the semantic type of a value.
        
        Returns one of:
        - 'null': None/null values
        - 'boolean': True/False
        - 'integer': Whole numbers
        - 'float': Decimal numbers
        - 'ip_address': IP address strings (e.g., "192.168.1.1")
        - 'uuid': UUID strings
        - 'email': Email addresses
        - 'url': HTTP/HTTPS URLs
        - 'timestamp': ISO timestamp strings
        - 'string': Generic strings
        - 'list': Arrays
        - 'dict': Nested objects
        """
        
        # Null check
        if value is None:
            return 'null'
        
        # Boolean check (must come before int, as bool is subclass of int in Python)
        if isinstance(value, bool):
            return 'boolean'
        
        # Integer check
        if isinstance(value, int):
            return 'integer'
        
        # Float check
        if isinstance(value, float):
            return 'float'
        
        # List check
        if isinstance(value, list):
            return 'list'
        
        # Dictionary check (nested object)
        if isinstance(value, dict):
            return 'dict'
        
        # String checks - semantic analysis
        if isinstance(value, str):
            # Empty string
            if not value:
                return 'string'
            
            # UUID check
            if TypeDetector.UUID_PATTERN.match(value):
                return 'uuid'
            
            # IP address check (must come before float-like checks)
            if TypeDetector.IP_PATTERN.match(value):
                # Validate IP parts are 0-255
                parts = value.split('.')
                if all(0 <= int(part) <= 255 for part in parts):
                    return 'ip_address'
            
            # Email check
            if TypeDetector.EMAIL_PATTERN.match(value):
                return 'email'
            
            # URL check
            if TypeDetector.URL_PATTERN.match(value):
                return 'url'
            
            # Timestamp check
            if TypeDetector.ISO_TIMESTAMP_PATTERN.match(value):
                return 'timestamp'
            
            # Generic string
            return 'string'
        
        # Fallback
        return 'unknown'
    
    @staticmethod
    def is_sql_compatible_type(detected_type: str) -> bool:
        """
        Determine if a type is naturally SQL-compatible (atomic/scalar).
        """
        sql_types = {
            'null', 'boolean', 'integer', 'float', 
            'ip_address', 'uuid', 'email', 'url', 
            'timestamp', 'string'
        }
        return detected_type in sql_types
    
    @staticmethod
    def is_unique_candidate(field_name: str, values_sample: List[Any]) -> bool:
        """
        Heuristic to determine if a field could be a unique identifier.
        
        Criteria:
        - Field name contains 'id', 'uuid', or 'key'
        - High diversity in sample values
        - Types are UUID, or integer/string with high cardinality
        """
        field_lower = field_name.lower()
        unique_indicators = ['id', 'uuid', 'key', 'session']
        
        # Check field name
        has_unique_name = any(indicator in field_lower for indicator in unique_indicators)
        
        if not values_sample or len(values_sample) < 2:
            return has_unique_name
        
        # Check uniqueness ratio
        unique_ratio = len(set(values_sample)) / len(values_sample)
        
        # High uniqueness suggests unique constraint candidate
        return has_unique_name and unique_ratio > 0.9
    
    @staticmethod
    def get_sql_type(detected_type: str) -> str:
        """
        Map semantic type to SQL column type.
        """
        type_mapping = {
            'null': 'TEXT',  # Will be updated based on actual values
            'boolean': 'BOOLEAN',
            'integer': 'INTEGER',
            'float': 'REAL',
            'ip_address': 'VARCHAR(15)',
            'uuid': 'VARCHAR(36)',
            'email': 'VARCHAR(255)',
            'url': 'TEXT',
            'timestamp': 'TIMESTAMP',
            'string': 'TEXT',
        }
        return type_mapping.get(detected_type, 'TEXT')
    
    @staticmethod
    def flatten_nested_fields(obj: dict, parent_key: str = '', sep: str = '_') -> dict:
        """
        Flatten nested dictionaries for SQL storage.
        
        Example:
            {'metadata': {'sensor': {'version': '2.1'}}}
            -> {'metadata_sensor_version': '2.1'}
        """
        items = []
        for k, v in obj.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            
            if isinstance(v, dict):
                items.extend(TypeDetector.flatten_nested_fields(v, new_key, sep).items())
            elif isinstance(v, list):
                # Convert lists to JSON strings for SQL
                import json
                items.append((new_key, json.dumps(v)))
            else:
                items.append((new_key, v))
        
        return dict(items)


# Example usage and testing
if __name__ == "__main__":
    detector = TypeDetector()
    
    # Test cases showcasing the difference between parsing
    test_cases = [
        ("192.168.1.1", "Should be ip_address, not string"),
        ("1.2.3.4", "Should be ip_address"),
        (1.2, "Should be float"),
        ("1.2", "Should be string (not ip_address - only 2 parts)"),
        ("user@example.com", "Should be email"),
        ("https://example.com", "Should be url"),
        ("550e8400-e29b-41d4-a716-446655440000", "Should be uuid"),
        ("2024-01-15T10:30:00", "Should be timestamp"),
        (True, "Should be boolean"),
        (42, "Should be integer"),
        (None, "Should be null"),
        ([1, 2, 3], "Should be list"),
        ({"key": "value"}, "Should be dict"),
    ]
    
    print("Type Detection Tests:")
    print("-" * 70)
    for value, description in test_cases:
        detected = detector.detect_type(value)
        print(f"{str(value):30} -> {detected:15} | {description}")

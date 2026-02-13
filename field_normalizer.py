"""
Field Normalizer - Resolves key ambiguity and naming inconsistencies
Handles cases like: ip, IP, IpAddress, user_name, userName, etc.
"""
import re
from typing import Dict, Any


class FieldNormalizer:
    """
    Dynamically normalizes field names to prevent duplicate columns.
    
    Rules:
    1. Convert to lowercase
    2. Standardize separators (underscore)
    3. Remove redundant prefixes/suffixes
    4. Apply semantic equivalence rules
    """
    
    def __init__(self):
        # Semantic equivalence mappings - these are common patterns
        self.semantic_patterns = {
            # IP patterns
            r'^ip(_?addr(ess)?)?$': 'ip_address',
            r'^ipv4(_?addr(ess)?)?$': 'ip_address',
            
            # User patterns
            r'^user(_?name)?$': 'username',
            r'^user_id$': 'user_id',
            
            # Email patterns
            r'^e?_?mail(_?addr(ess)?)?$': 'email',
            
            # Phone patterns
            r'^(phone|tel|telephone)(_?num(ber)?)?$': 'phone',
            
            # Timestamp patterns
            r'^(time)?_?stamp$': 'timestamp',
            r'^t_?stamp$': 'timestamp',
            r'^created(_?at)?$': 'created_at',
            r'^updated(_?at)?$': 'updated_at',
            
            # GPS patterns
            r'^(gps_?)?(lat|latitude)$': 'gps_lat',
            r'^(gps_?)?(lon|long|longitude)$': 'gps_lon',
            
            # Device patterns
            r'^dev(ice)?_?id$': 'device_id',
            r'^dev(ice)?_?model$': 'device_model',
            
            # Session patterns
            r'^sess(ion)?_?id$': 'session_id',
            
            # Network patterns
            r'^net(work)?$': 'network',
            
            # Battery patterns  
            r'^bat(tery)?(_?level)?$': 'battery',
            
            # OS patterns
            r'^os(_?name)?$': 'os',
            r'^operating_?system$': 'os',
            
            # Version patterns
            r'^(app_?)version$': 'app_version',
            r'^ver(sion)?$': 'version',
        }
        
        # Compile patterns for efficiency
        self.compiled_patterns = {
            re.compile(pattern, re.IGNORECASE): normalized 
            for pattern, normalized in self.semantic_patterns.items()
        }
    
    def normalize(self, field_name: str) -> str:
        """
        Normalize a field name according to rules:
        1. Convert camelCase/PascalCase to snake_case
        2. Remove extra underscores
        3. Convert to lowercase
        4. Apply semantic equivalence patterns
        """
        if not field_name:
            return field_name
        
        # Step 1: Convert camelCase/PascalCase to snake_case
        # Insert underscore before uppercase letters
        s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', field_name)
        normalized = re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1)
        
        # Step 2: Convert to lowercase
        normalized = normalized.lower()
        
        # Step 3: Remove multiple consecutive underscores
        normalized = re.sub('_+', '_', normalized)
        
        # Step 4: Strip leading/trailing underscores
        normalized = normalized.strip('_')
        
        # Step 5: Apply semantic equivalence patterns
        for pattern, standard_name in self.compiled_patterns.items():
            if pattern.match(normalized):
                return standard_name
        
        return normalized
    
    def normalize_keys(self, record: Dict[str, Any], preserve_original: bool = False) -> Dict[str, Any]:
        """
        Normalize all keys in a dictionary (including nested).
        
        Args:
            record: The dictionary to normalize
            preserve_original: If True, keeps both original and normalized keys
        
        Returns:
            Dictionary with normalized keys
        """
        normalized_record = {}
        
        for key, value in record.items():
            normalized_key = self.normalize(key)
            
            # Handle nested dictionaries recursively
            if isinstance(value, dict):
                value = self.normalize_keys(value, preserve_original)
            
            # Handle lists of dictionaries
            elif isinstance(value, list):
                value = [
                    self.normalize_keys(item, preserve_original) 
                    if isinstance(item, dict) else item 
                    for item in value
                ]
            
            normalized_record[normalized_key] = value
            
            # Optionally preserve original key
            if preserve_original and normalized_key != key:
                normalized_record[f'_original_{key}'] = value
        
        return normalized_record
    
    def get_normalization_mapping(self, field_name: str) -> tuple:
        """
        Get the normalization mapping for a field.
        Returns: (original_name, normalized_name)
        """
        normalized = self.normalize(field_name)
        return (field_name, normalized)


# Example usage and testing
if __name__ == "__main__":
    normalizer = FieldNormalizer()
    
    # Test cases
    test_cases = [
        "ip", "IP", "IpAddress", "ip_address", "ipAddress",
        "userName", "user_name", "username", "UserName",
        "emailAddress", "email", "eMail",
        "phoneNumber", "phone", "Phone",
        "deviceId", "device_id", "DeviceID",
        "timestamp", "timeStamp", "t_stamp", "tStamp",
        "gpsLat", "gps_lat", "latitude", "Latitude",
        "sessionId", "session_id", "SessionID"
    ]
    
    print("Field Normalization Tests:")
    print("-" * 50)
    for test in test_cases:
        normalized = normalizer.normalize(test)
        print(f"{test:20} -> {normalized}")

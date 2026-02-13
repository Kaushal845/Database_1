"""
Metadata Store - Persists field classification decisions across restarts
"""
import json
import os
from typing import Dict, Any, List
from datetime import datetime
import threading


class MetadataStore:
    """
    Persistent storage for field classifications, frequency tracking, and type stability.
    This allows the system to remember decisions across restarts.
    """
    
    def __init__(self, storage_file='metadata_store.json'):
        self.storage_file = storage_file
        self.lock = threading.Lock()
        self.metadata = self._load_metadata()
        
    def _load_metadata(self) -> Dict[str, Any]:
        """Load metadata from disk or initialize if not exists"""
        if os.path.exists(self.storage_file):
            try:
                with open(self.storage_file, 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error loading metadata: {e}. Initializing fresh.")
                return self._initialize_metadata()
        return self._initialize_metadata()
    
    def _initialize_metadata(self) -> Dict[str, Any]:
        """Initialize empty metadata structure"""
        return {
            'fields': {},  # Field-level tracking
            'normalization_rules': {},  # Mapping of observed keys to normalized keys
            'placement_decisions': {},  # SQL vs MongoDB decisions
            'total_records': 0,
            'last_updated': datetime.utcnow().isoformat(),
            'session_start': datetime.utcnow().isoformat()
        }
    
    def save(self):
        """Persist metadata to disk"""
        with self.lock:
            self.metadata['last_updated'] = datetime.utcnow().isoformat()
            try:
                with open(self.storage_file, 'w') as f:
                    json.dump(self.metadata, f, indent=2)
            except Exception as e:
                print(f"Error saving metadata: {e}")
    
    def increment_total_records(self):
        """Increment total record count"""
        with self.lock:
            self.metadata['total_records'] += 1
    
    def update_field_stats(self, normalized_key: str, data_type: str, value: Any):
        """Update statistics for a specific field"""
        with self.lock:
            if normalized_key not in self.metadata['fields']:
                self.metadata['fields'][normalized_key] = {
                    'appearances': 0,
                    'type_counts': {},
                    'first_seen': datetime.utcnow().isoformat(),
                    'last_seen': datetime.utcnow().isoformat(),
                    'sample_values': []
                }
            
            field_data = self.metadata['fields'][normalized_key]
            field_data['appearances'] += 1
            field_data['last_seen'] = datetime.utcnow().isoformat()
            
            # Track type occurrences
            if data_type not in field_data['type_counts']:
                field_data['type_counts'][data_type] = 0
            field_data['type_counts'][data_type] += 1
            
            # Keep sample values (max 5)
            if len(field_data['sample_values']) < 5:
                if value not in field_data['sample_values']:
                    field_data['sample_values'].append(str(value)[:100])  # Truncate long values
    
    def add_normalization_rule(self, original_key: str, normalized_key: str):
        """Store a normalization mapping"""
        with self.lock:
            self.metadata['normalization_rules'][original_key] = normalized_key
    
    def get_normalized_key(self, original_key: str) -> str:
        """Get normalized key for an original key"""
        return self.metadata['normalization_rules'].get(original_key, original_key)
    
    def set_placement_decision(self, normalized_key: str, backend: str, reason: str):
        """Store placement decision for a field"""
        with self.lock:
            self.metadata['placement_decisions'][normalized_key] = {
                'backend': backend,  # 'SQL' or 'MongoDB'
                'reason': reason,
                'decided_at': datetime.utcnow().isoformat()
            }
    
    def get_placement_decision(self, normalized_key: str) -> Dict[str, str]:
        """Get placement decision for a field"""
        return self.metadata['placement_decisions'].get(normalized_key)
    
    def get_field_frequency(self, normalized_key: str) -> float:
        """Calculate frequency percentage for a field"""
        if self.metadata['total_records'] == 0:
            return 0.0
        field_data = self.metadata['fields'].get(normalized_key)
        if not field_data:
            return 0.0
        return (field_data['appearances'] / self.metadata['total_records']) * 100
    
    def get_field_type_stability(self, normalized_key: str) -> tuple:
        """
        Get the dominant type and stability percentage for a field.
        Returns: (dominant_type, stability_percentage)
        """
        field_data = self.metadata['fields'].get(normalized_key)
        if not field_data or not field_data['type_counts']:
            return ('unknown', 0.0)
        
        total_appearances = sum(field_data['type_counts'].values())
        dominant_type = max(field_data['type_counts'], key=field_data['type_counts'].get)
        dominant_count = field_data['type_counts'][dominant_type]
        
        stability = (dominant_count / total_appearances) * 100
        return (dominant_type, stability)
    
    def get_all_fields(self) -> List[str]:
        """Get list of all tracked normalized field names"""
        return list(self.metadata['fields'].keys())
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics for reporting"""
        return {
            'total_records': self.metadata['total_records'],
            'unique_fields': len(self.metadata['fields']),
            'normalization_rules': len(self.metadata['normalization_rules']),
            'placement_decisions': len(self.metadata['placement_decisions']),
            'session_start': self.metadata['session_start'],
            'last_updated': self.metadata['last_updated']
        }

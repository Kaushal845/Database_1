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
    Auto-saves after each record update to ensure persistence.
    """
    
    def __init__(self, storage_file='metadata_store.json', auto_save=True):
        self.storage_file = storage_file
        self.auto_save = auto_save
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
            'placement_decisions': {},  # SQL vs MongoDB decisions
            'current_placement': {},  # Current placement of each field (SQL/MongoDB)
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
        """Increment total record count and auto-save if enabled"""
        with self.lock:
            self.metadata['total_records'] += 1
        if self.auto_save:
            self.save()
    
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
        
        if self.auto_save:
            self.save()
    
    def set_placement_decision(self, normalized_key: str, backend: str, reason: str):
        """Store placement decision for a field and update current placement"""
        with self.lock:
            self.metadata['placement_decisions'][normalized_key] = {
                'backend': backend,  # 'SQL' or 'MongoDB'
                'reason': reason,
                'decided_at': datetime.utcnow().isoformat()
            }
            # Update current placement
            self.metadata['current_placement'][normalized_key] = backend
        
        if self.auto_save:
            self.save()
    
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
            'placement_decisions': len(self.metadata['placement_decisions']),
            'session_start': self.metadata['session_start'],
            'last_updated': self.metadata['last_updated']
        }
    
    def get_field_stats(self, normalized_key: str) -> Dict[str, Any]:
        """
        Get comprehensive statistics for a field including frequency,
        type stability, and drift information.
        """
        field_data = self.metadata['fields'].get(normalized_key)
        if not field_data:
            return {
                'frequency': 0.0,
                'type_stability': 0.0,
                'drift_score': 0.0,
                'appearances': 0,
                'null_ratio': 1.0,
                'dominant_type': 'unknown'
            }
        
        # Calculate frequency
        frequency = self.get_field_frequency(normalized_key)
        
        # Calculate type stability and drift
        dominant_type, type_stability = self.get_field_type_stability(normalized_key)
        drift_score = (100.0 - type_stability) / 100.0  # Convert to 0-1 range
        
        # Calculate null ratio (if tracked)
        total_records = self.metadata['total_records']
        appearances = field_data['appearances']
        null_ratio = 1.0 - (appearances / total_records) if total_records > 0 else 1.0
        
        return {
            'frequency': frequency,
            'type_stability': type_stability,
            'drift_score': drift_score,
            'appearances': appearances,
            'null_ratio': null_ratio,
            'dominant_type': dominant_type,
            'type_counts': field_data.get('type_counts', {})
        }
    
    def mark_quarantined(self, normalized_key: str, drift_score: float):
        """Mark a field as quarantined due to severe type drift"""
        with self.lock:
            if 'quarantined_fields' not in self.metadata:
                self.metadata['quarantined_fields'] = {}
            
            self.metadata['quarantined_fields'][normalized_key] = {
                'drift_score': drift_score,
                'quarantined_at': datetime.utcnow().isoformat(),
                'reason': f"Severe type drift detected (score: {drift_score:.2f})"
            }
        
        if self.auto_save:
            self.save()
    
    def is_quarantined(self, normalized_key: str) -> bool:
        """Check if a field is quarantined"""
        quarantined = self.metadata.get('quarantined_fields', {})
        return normalized_key in quarantined
    
    def get_field_placement(self, normalized_key: str) -> str:
        """Get current placement of a field (SQL or MongoDB)"""
        return self.metadata.get('current_placement', {}).get(normalized_key, 'unknown')
    
    def get_fields_by_placement(self, backend: str) -> List[str]:
        """Get all fields currently placed in a specific backend (SQL or MongoDB)"""
        current_placement = self.metadata.get('current_placement', {})
        return [field for field, placement in current_placement.items() if placement == backend]
    
    def get_placement_summary(self) -> Dict[str, Any]:
        """Get summary of field placements across backends"""
        current_placement = self.metadata.get('current_placement', {})
        sql_fields = [f for f, p in current_placement.items() if p == 'SQL']
        mongo_fields = [f for f, p in current_placement.items() if p == 'MongoDB']
        
        return {
            'sql_field_count': len(sql_fields),
            'mongodb_field_count': len(mongo_fields),
            'sql_fields': sql_fields,
            'mongodb_fields': mongo_fields
        }

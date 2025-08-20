import hashlib


class UUIDConverter:
    """
    Centralized UUID to integer conversion utility
    
    This class provides consistent UUID-to-integer conversion methods
    to ensure foreign key relationships work correctly across all OMOP tables.
    """
    
    @staticmethod
    def person_id(uuid_str: str) -> int:
        """
        Convert UUID to integer using the Person table algorithm
        
        Used for:
        - person.person_id
        - visit_occurrence.person_id (FK reference)
        - Any other FK references to person
        
        Args:
            uuid_str: UUID string to convert
            
        Returns:
            Integer in range 0 to 2^31-2
        """
        raw_hash = int(hashlib.md5(str(uuid_str).encode()).hexdigest()[:8], 16)
        return raw_hash % (2**31 - 1)
    
    @staticmethod
    def provider_id(uuid_str: str) -> int:
        """
        Convert UUID to integer using the Provider table algorithm
        
        Used for:
        - provider.provider_id
        - visit_occurrence.provider_id (FK reference)
        - Any other FK references to provider
        
        Args:
            uuid_str: UUID string to convert
            
        Returns:
            Integer in range 1 to 2147483647
        """
        hash_bytes = hashlib.md5(str(uuid_str).encode()).digest()[:4]
        unsigned_int = int.from_bytes(hash_bytes, byteorder='big', signed=False)
        return unsigned_int % 2147483647 + 1
    
    @staticmethod
    def care_site_id(uuid_str: str) -> int:
        """
        Convert UUID to integer using the Care Site table algorithm
        
        Used for:
        - care_site.care_site_id
        - provider.care_site_id (FK reference)
        - visit_occurrence.care_site_id (FK reference)
        - Any other FK references to care_site
        
        Args:
            uuid_str: UUID string to convert
            
        Returns:
            Integer in range 1 to 2147483647
        """
        hash_bytes = hashlib.md5(str(uuid_str).encode()).digest()[:4]
        unsigned_int = int.from_bytes(hash_bytes, byteorder='big', signed=False)
        return unsigned_int % 2147483647 + 1
    
    @staticmethod
    def location_id(uuid_str: str) -> int:
        """
        Convert UUID to integer using the Location table algorithm
        
        Used for:
        - location.location_id
        - person.location_id (FK reference)
        - provider.location_id (FK reference)
        - Any other FK references to location
        
        Args:
            uuid_str: UUID string to convert
            
        Returns:
            Integer - currently using location transformer's UUID5 method
        """
        # Note: Location uses a different method (UUID5)
        # This is a placeholder - location uses uuid.uuid5() in LocationTransformer
        # We may want to standardize this later
        import uuid
        return uuid.uuid5(uuid.NAMESPACE_DNS, str(uuid_str)).int % (10 ** 9)
    
    @staticmethod
    def visit_occurrence_id(uuid_str: str) -> int:
        """
        Convert UUID to integer for visit occurrence IDs
        
        Used for:
        - visit_occurrence.visit_occurrence_id
        - Any FK references to visit_occurrence
        
        Args:
            uuid_str: UUID string to convert
            
        Returns:
            Integer in range 1 to 2147483647
        """
        # Use same algorithm as provider/care_site for consistency
        hash_bytes = hashlib.md5(str(uuid_str).encode()).digest()[:4]
        unsigned_int = int.from_bytes(hash_bytes, byteorder='big', signed=False)
        return unsigned_int % 2147483647 + 1
    
    @staticmethod
    def generic_id(uuid_str: str) -> int:
        """
        Convert UUID to integer using the standard algorithm
        
        Used for new tables where we want to use the "standard" approach.
        Uses the provider/care_site algorithm as the default.
        
        Args:
            uuid_str: UUID string to convert
            
        Returns:
            Integer in range 1 to 2147483647
        """
        hash_bytes = hashlib.md5(str(uuid_str).encode()).digest()[:4]
        unsigned_int = int.from_bytes(hash_bytes, byteorder='big', signed=False)
        return unsigned_int % 2147483647 + 1


# Convenience functions for backward compatibility
def uuid_to_person_id(uuid_str: str) -> int:
    """Backward compatibility function"""
    return UUIDConverter.person_id(uuid_str)

def uuid_to_provider_id(uuid_str: str) -> int:
    """Backward compatibility function"""
    return UUIDConverter.provider_id(uuid_str)

def uuid_to_care_site_id(uuid_str: str) -> int:
    """Backward compatibility function"""
    return UUIDConverter.care_site_id(uuid_str)
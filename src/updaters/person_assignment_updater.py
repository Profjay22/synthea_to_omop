import pandas as pd
from sqlalchemy import text
from src.database.connection import DatabaseManager


class PersonAssignmentUpdater:
    """Update person table with provider and care site assignments from visit data"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def update_assignments(self) -> bool:
        """
        Update person table with provider_id and care_site_id based on visit_occurrence data.
        Uses the most recent visit for each patient to determine their primary provider and care site.
        """
        try:
            print("🔄 Updating person table with provider and care site assignments...")
            
            # Get the most recent visit for each person
            most_recent_visits_query = f"""
            WITH most_recent_visits AS (
                SELECT DISTINCT
                    person_id,
                    FIRST_VALUE(provider_id) OVER (
                        PARTITION BY person_id 
                        ORDER BY visit_start_date DESC, visit_start_datetime DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as latest_provider_id,
                    FIRST_VALUE(care_site_id) OVER (
                        PARTITION BY person_id 
                        ORDER BY visit_start_date DESC, visit_start_datetime DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as latest_care_site_id
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE provider_id IS NOT NULL 
                   OR care_site_id IS NOT NULL
            )
            SELECT DISTINCT person_id, latest_provider_id, latest_care_site_id
            FROM most_recent_visits
            """
            
            print("📊 Finding most recent provider and care site for each patient...")
            assignments = self.db_manager.execute_query(most_recent_visits_query)
            
            if assignments.empty:
                print("⚠️ No visit data found to update person assignments")
                return True
            
            print(f"✅ Found assignments for {len(assignments)} patients")
            
            # Update person table in batches
            update_count = 0
            batch_size = 100
            
            for i in range(0, len(assignments), batch_size):
                batch = assignments.iloc[i:i+batch_size]
                
                for _, row in batch.iterrows():
                    # Convert numpy types to native Python types to avoid psycopg2 adaptation errors
                    person_id = int(row['person_id']) if pd.notna(row['person_id']) else None
                    provider_id = int(row['latest_provider_id']) if pd.notna(row['latest_provider_id']) else None
                    care_site_id = int(row['latest_care_site_id']) if pd.notna(row['latest_care_site_id']) else None
                    
                    # Update the person record
                    update_query = f"""
                    UPDATE {self.db_manager.config.schema_cdm}.person 
                    SET provider_id = :provider_id,
                        care_site_id = :care_site_id
                    WHERE person_id = :person_id
                    """
                    
                    with self.db_manager.engine.begin() as conn:
                        conn.execute(text(update_query), {
                            'provider_id': provider_id,
                            'care_site_id': care_site_id,
                            'person_id': person_id
                        })
                    
                    update_count += 1
                
                print(f"   ✅ Updated batch {i//batch_size + 1} ({len(batch)} records)")
            
            print(f"✅ Successfully updated {update_count} person records with provider/care site assignments")
            
            # Verify the updates
            self._verify_updates()
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to update person assignments: {e}")
            return False
    
    def _verify_updates(self):
        """Verify the person table updates"""
        try:
            verification_query = f"""
            SELECT 
                COUNT(*) as total_persons,
                COUNT(provider_id) as persons_with_provider,
                COUNT(care_site_id) as persons_with_care_site,
                COUNT(CASE WHEN provider_id IS NOT NULL AND care_site_id IS NOT NULL THEN 1 END) as persons_with_both
            FROM {self.db_manager.config.schema_cdm}.person
            """
            
            result = self.db_manager.execute_query(verification_query).iloc[0]
            
            print("\n🔍 Person table update verification:")
            print(f"  Total persons: {result['total_persons']}")
            print(f"  With provider_id: {result['persons_with_provider']}")
            print(f"  With care_site_id: {result['persons_with_care_site']}")
            print(f"  With both: {result['persons_with_both']}")
            
            # Show sample updated records
            sample_query = f"""
            SELECT person_id, provider_id, care_site_id, location_id
            FROM {self.db_manager.config.schema_cdm}.person 
            WHERE provider_id IS NOT NULL OR care_site_id IS NOT NULL
            LIMIT 5
            """
            
            samples = self.db_manager.execute_query(sample_query)
            print("\n📋 Sample updated person records:")
            for _, r in samples.iterrows():
                print(f"  Person: {r['person_id']} | Provider: {r['provider_id']} | "
                      f"Care Site: {r['care_site_id']} | Location: {r['location_id']}")
                
        except Exception as e:
            print(f"⚠️ Verification failed: {e}")


# Alternative: More efficient bulk update approach
class PersonAssignmentUpdaterBulk:
    """More efficient version using bulk updates"""
    
    def __init__(self, db_manager: DatabaseManager):
        self.db_manager = db_manager
    
    def update_assignments(self) -> bool:
        """
        Update person table using a single bulk UPDATE statement with JOIN.
        More efficient for large datasets.
        """
        try:
            print("🔄 Updating person table with provider and care site assignments (bulk method)...")
            
            # Single bulk update using WITH clause and UPDATE...FROM
            bulk_update_query = f"""
            WITH most_recent_visits AS (
                SELECT DISTINCT
                    person_id,
                    FIRST_VALUE(provider_id) OVER (
                        PARTITION BY person_id 
                        ORDER BY visit_start_date DESC, visit_start_datetime DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as latest_provider_id,
                    FIRST_VALUE(care_site_id) OVER (
                        PARTITION BY person_id 
                        ORDER BY visit_start_date DESC, visit_start_datetime DESC
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) as latest_care_site_id
                FROM {self.db_manager.config.schema_cdm}.visit_occurrence
                WHERE provider_id IS NOT NULL 
                   OR care_site_id IS NOT NULL
            ),
            assignments AS (
                SELECT DISTINCT person_id, latest_provider_id, latest_care_site_id
                FROM most_recent_visits
            )
            UPDATE {self.db_manager.config.schema_cdm}.person 
            SET 
                provider_id = assignments.latest_provider_id,
                care_site_id = assignments.latest_care_site_id
            FROM assignments
            WHERE person.person_id = assignments.person_id
            """
            
            print("📊 Executing bulk update...")
            with self.db_manager.engine.begin() as conn:
                result = conn.execute(text(bulk_update_query))
                update_count = result.rowcount
            
            print(f"✅ Successfully updated {update_count} person records with provider/care site assignments")
            
            # Verify the updates
            self._verify_updates()
            
            return True
            
        except Exception as e:
            print(f"❌ Failed to update person assignments: {e}")
            return False
    
    def _verify_updates(self):
        """Verify the person table updates"""
        try:
            verification_query = f"""
            SELECT 
                COUNT(*) as total_persons,
                COUNT(provider_id) as persons_with_provider,
                COUNT(care_site_id) as persons_with_care_site,
                COUNT(CASE WHEN provider_id IS NOT NULL AND care_site_id IS NOT NULL THEN 1 END) as persons_with_both
            FROM {self.db_manager.config.schema_cdm}.person
            """
            
            result = self.db_manager.execute_query(verification_query).iloc[0]
            
            print("\n🔍 Person table update verification:")
            print(f"  Total persons: {result['total_persons']}")
            print(f"  With provider_id: {result['persons_with_provider']}")
            print(f"  With care_site_id: {result['persons_with_care_site']}")
            print(f"  With both: {result['persons_with_both']}")
            
        except Exception as e:
            print(f"⚠️ Verification failed: {e}")
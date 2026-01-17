#!/usr/bin/env python3
"""
Export OMOP CDM tables from PostgreSQL to CSV files
Creates a folder structure with each table as a separate file
"""

import os
import argparse
import pandas as pd
from datetime import datetime
from sqlalchemy import text
from config.database import DatabaseConfig
from src.database.connection import DatabaseManager
from src.utils.logging import setup_logging

class OMOPExporter:
    def __init__(self, output_dir: str = "omop_export"):
        self.output_dir = output_dir
        self.logger = setup_logging(log_level="INFO")
        
        # Initialize database connection
        self.db_config = DatabaseConfig.from_env()
        self.db_manager = DatabaseManager(self.db_config)
        
        # Define OMOP tables to export
        self.omop_tables = [
            'person',
            'location',
            'care_site',
            'provider',
            'visit_occurrence',
            'condition_occurrence',
            'observation',
            'observation_period',
            'procedure_occurrence',
            'death',
            'drug_exposure',
            'measurement',
            'condition_era',
            'drug_era',
            'dose_era'
        ]
        
        self.export_stats = {}

    def create_output_directory(self):
        """Create output directory with timestamp"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        full_output_dir = f"{self.output_dir}_{timestamp}"
        
        os.makedirs(full_output_dir, exist_ok=True)
        self.output_dir = full_output_dir
        
        self.logger.info(f"Created export directory: {full_output_dir}")
        return full_output_dir

    def get_table_count(self, table_name: str) -> int:
        """Get row count for a table"""
        try:
            query = text(f"SELECT COUNT(*) FROM {self.db_config.schema_cdm}.{table_name}")
            with self.db_manager.engine.connect() as conn:
                count = conn.execute(query).scalar()
            return count
        except Exception as e:
            self.logger.warning(f"Could not get count for {table_name}: {e}")
            return 0

    def export_table(self, table_name: str) -> bool:
        """Export a single table to CSV"""
        try:
            self.logger.info(f"Exporting {table_name}...")
            
            # Get row count first
            row_count = self.get_table_count(table_name)
            
            if row_count == 0:
                self.logger.warning(f"Table {table_name} is empty, skipping")
                self.export_stats[table_name] = {'rows': 0, 'status': 'empty'}
                return True
            
            self.logger.info(f"  Found {row_count:,} rows")
            
            # Export table data
            query = f"SELECT * FROM {self.db_config.schema_cdm}.{table_name}"
            df = self.db_manager.execute_query(query)
            
            # Save to CSV
            output_file = os.path.join(self.output_dir, f"{table_name}.csv")
            df.to_csv(output_file, index=False)
            
            self.logger.info(f"  Exported to {output_file}")
            self.export_stats[table_name] = {'rows': len(df), 'status': 'success'}
            
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export {table_name}: {e}")
            self.export_stats[table_name] = {'rows': 0, 'status': 'failed', 'error': str(e)}
            return False

    def export_all_tables(self, tables_to_export: list = None) -> bool:
        """Export all specified tables"""
        if tables_to_export is None:
            tables_to_export = self.omop_tables
            
        self.logger.info("Starting OMOP database export")
        self.logger.info(f"Tables to export: {tables_to_export}")
        self.logger.info("=" * 60)
        
        # Test database connection
        if not self.db_manager.test_connection():
            self.logger.error("Database connection failed")
            return False
            
        # Create output directory
        self.create_output_directory()
        
        # Export each table
        success_count = 0
        for table in tables_to_export:
            if self.export_table(table):
                success_count += 1
        
        # Generate summary
        self.generate_summary()
        
        self.logger.info(f"Export completed: {success_count}/{len(tables_to_export)} tables exported successfully")
        return success_count == len(tables_to_export)

    def generate_summary(self):
        """Generate export summary report"""
        summary_file = os.path.join(self.output_dir, "export_summary.txt")
        
        with open(summary_file, 'w') as f:
            f.write("OMOP Database Export Summary\n")
            f.write("=" * 40 + "\n")
            f.write(f"Export Date: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Database Schema: {self.db_config.schema_cdm}\n")
            f.write(f"Output Directory: {self.output_dir}\n\n")
            
            f.write("Table Export Results:\n")
            f.write("-" * 40 + "\n")
            
            total_rows = 0
            successful_tables = 0
            
            for table_name, stats in self.export_stats.items():
                status = stats['status']
                rows = stats['rows']
                
                if status == 'success':
                    f.write(f"{table_name:<25} {rows:>10,} rows   ✓\n")
                    total_rows += rows
                    successful_tables += 1
                elif status == 'empty':
                    f.write(f"{table_name:<25} {'EMPTY':>10}        -\n")
                    successful_tables += 1
                else:
                    error = stats.get('error', 'Unknown error')
                    f.write(f"{table_name:<25} {'FAILED':>10}        ✗ ({error})\n")
            
            f.write("-" * 40 + "\n")
            f.write(f"Total Tables: {len(self.export_stats)}\n")
            f.write(f"Successful: {successful_tables}\n") 
            f.write(f"Total Rows Exported: {total_rows:,}\n")
        
        self.logger.info(f"Summary report saved to: {summary_file}")

    def export_custom_query(self, query: str, filename: str):
        """Export results of a custom query"""
        try:
            self.logger.info(f"Executing custom query for {filename}")
            df = self.db_manager.execute_query(query)
            
            output_file = os.path.join(self.output_dir, f"{filename}.csv")
            df.to_csv(output_file, index=False)
            
            self.logger.info(f"Custom query results exported to {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export custom query: {e}")
            return False

def main():
    parser = argparse.ArgumentParser(description='Export OMOP CDM tables to CSV files')
    parser.add_argument('--output-dir', default='omop_export', help='Output directory name (default: omop_export)')
    parser.add_argument('--tables', nargs='+', help='Specific tables to export (default: all)')
    parser.add_argument('--include-vocab', action='store_true', help='Include vocabulary tables (concept, concept_relationship, etc.)')
    
    args = parser.parse_args()
    
    # Initialize exporter
    exporter = OMOPExporter(output_dir=args.output_dir)
    
    # Add vocabulary tables if requested
    if args.include_vocab:
        vocab_tables = ['concept', 'vocabulary', 'domain', 'concept_class', 'concept_relationship', 'relationship', 'concept_synonym', 'concept_ancestor']
        exporter.omop_tables.extend(vocab_tables)
    
    # Determine tables to export
    tables_to_export = args.tables if args.tables else exporter.omop_tables
    
    # Run export
    success = exporter.export_all_tables(tables_to_export)
    
    if success:
        print(f"\nExport completed successfully!")
        print(f"Files saved in: {exporter.output_dir}")
    else:
        print(f"\nExport completed with errors")
        print(f"Check logs and summary in: {exporter.output_dir}")
        exit(1)

if __name__ == "__main__":
    main()
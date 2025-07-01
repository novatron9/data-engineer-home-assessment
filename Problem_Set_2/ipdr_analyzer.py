#!/usr/bin/env python3
"""
Problem Set-2: IPDR Audio and Video Call Analysis

Analyzes IPDR (IP Detail Record) data to identify and classify VoIP calls
as audio or video based on bitrate analysis.

Requirements:
1. Group FDRs by MSISDN and domain
2. Identify individual calls within each domain
3. Calculate call duration and volume
4. Determine bitrate and classify as audio/video
5. Filter out calls with bitrate < 10 kbps
6. Audio calls: <= 200 kbps, Video calls: > 200 kbps
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from typing import List, Dict, Tuple


class IPDRAnalyzer:
    def __init__(self, csv_file: str):
        """Initialize with IPDR CSV file"""
        self.df = pd.read_csv(csv_file)
        self.prepare_data()
    
    def prepare_data(self):
        """Prepare and clean the data"""
        # Fix datetime format (remove space between date and time)
        self.df['starttime'] = self.df['starttime'].str.replace(r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})', r'\1 \2', regex=True)
        self.df['endtime'] = self.df['endtime'].str.replace(r'(\d{4}-\d{2}-\d{2})(\d{2}:\d{2}:\d{2})', r'\1 \2', regex=True)
        
        # Convert to datetime
        self.df['start_dt'] = pd.to_datetime(self.df['starttime'])
        self.df['end_dt'] = pd.to_datetime(self.df['endtime'])
        
        # Calculate duration in seconds
        self.df['duration_sec'] = (self.df['end_dt'] - self.df['start_dt']).dt.total_seconds()
        
        # Apply 10-minute idle time exclusion
        self.df['adjusted_end_dt'] = self.df['end_dt'] - timedelta(minutes=10)
        
        # If adjusted end time is before start time, keep original end time
        mask = self.df['adjusted_end_dt'] < self.df['start_dt']
        self.df.loc[mask, 'adjusted_end_dt'] = self.df.loc[mask, 'end_dt']
        
        # Recalculate adjusted duration
        self.df['adjusted_duration_sec'] = (self.df['adjusted_end_dt'] - self.df['start_dt']).dt.total_seconds()
        
        # Sort by MSISDN, domain, and start time
        self.df = self.df.sort_values(['msisdn', 'domain', 'start_dt'])
    
    def identify_calls(self) -> List[Dict]:
        """
        Identify individual calls by grouping consecutive FDRs
        Calls are separated by gaps in time within the same MSISDN-domain combination
        """
        calls = []
        
        # Group by MSISDN and domain
        for (msisdn, domain), group in self.df.groupby(['msisdn', 'domain']):
            group = group.sort_values('start_dt').reset_index()
            
            current_call_fdrs = []
            call_gap_threshold = timedelta(minutes=5)  # 5 minutes gap indicates new call
            
            for idx, row in group.iterrows():
                if not current_call_fdrs:
                    # Start new call
                    current_call_fdrs = [row]
                else:
                    # Check if this FDR belongs to current call or starts a new one
                    last_fdr = current_call_fdrs[-1]
                    
                    # If there's a significant gap, start new call
                    if row['start_dt'] - last_fdr['end_dt'] > call_gap_threshold:
                        # Process current call
                        call_data = self.process_call(current_call_fdrs, msisdn, domain)
                        if call_data:
                            calls.append(call_data)
                        
                        # Start new call
                        current_call_fdrs = [row]
                    else:
                        # Add to current call
                        current_call_fdrs.append(row)
            
            # Process the last call
            if current_call_fdrs:
                call_data = self.process_call(current_call_fdrs, msisdn, domain)
                if call_data:
                    calls.append(call_data)
        
        return calls
    
    def process_call(self, fdrs: List, msisdn: int, domain: str) -> Dict:
        """Process a single call composed of multiple FDRs"""
        if not fdrs:
            return None
        
        # Convert to DataFrame for easier processing
        call_df = pd.DataFrame(fdrs)
        
        # Calculate call metrics
        total_ul_volume = call_df['ulvolume'].sum()  # bytes
        total_dl_volume = call_df['dlvolume'].sum()  # bytes
        total_volume_kb = (total_ul_volume + total_dl_volume) / 1024  # Convert to KB
        
        # Calculate call duration: highest end time - lowest start time
        call_start = call_df['start_dt'].min()
        call_end = call_df['adjusted_end_dt'].max()
        call_duration_sec = (call_end - call_start).total_seconds()
        
        # Skip calls with zero or negative duration
        if call_duration_sec <= 0:
            return None
        
        # Calculate bitrate in kbps
        bitrate_kbps = (total_volume_kb * 8) / (call_duration_sec / 3600)  # kb per second
        
        # Filter out calls with bitrate < 10 kbps
        if bitrate_kbps < 10:
            return None
        
        # Classify as audio or video
        is_audio = bitrate_kbps <= 200
        is_video = bitrate_kbps > 200
        
        return {
            'msisdn': msisdn,
            'domain': domain,
            'duration_sec': int(call_duration_sec),
            'fdr_count': len(fdrs),
            'kbps': round(bitrate_kbps, 2),
            'is_audio': is_audio,
            'is_video': is_video,
            'total_volume_kb': round(total_volume_kb, 2),
            'call_start': call_start,
            'call_end': call_end
        }
    
    def analyze(self) -> pd.DataFrame:
        """Perform complete IPDR analysis"""
        calls = self.identify_calls()
        
        if not calls:
            return pd.DataFrame()
        
        # Convert to DataFrame
        results_df = pd.DataFrame(calls)
        
        # Reorder columns as specified in requirements
        columns_order = ['msisdn', 'domain', 'duration_sec', 'fdr_count', 'kbps', 'is_audio', 'is_video']
        return results_df[columns_order]
    
    def generate_summary(self, results_df: pd.DataFrame) -> Dict:
        """Generate analysis summary"""
        if results_df.empty:
            return {'total_calls': 0, 'audio_calls': 0, 'video_calls': 0}
        
        return {
            'total_calls': len(results_df),
            'audio_calls': results_df['is_audio'].sum(),
            'video_calls': results_df['is_video'].sum(),
            'avg_duration_sec': results_df['duration_sec'].mean(),
            'avg_bitrate_kbps': results_df['kbps'].mean(),
            'total_msisdns': results_df['msisdn'].nunique(),
            'total_domains': results_df['domain'].nunique()
        }


def main():
    """Main function"""
    if len(sys.argv) != 2:
        print("Usage: python3 ipdr_analyzer.py <ipdr_file.csv>")
        return 1
    
    csv_file = sys.argv[1]
    
    try:
        # Initialize analyzer
        analyzer = IPDRAnalyzer(csv_file)
        
        # Perform analysis
        results = analyzer.analyze()
        
        if results.empty:
            print("No valid calls found in the data.")
            return 0
        
        # Print results
        print("IPDR Call Analysis Results:")
        print("=" * 50)
        print(results.to_string(index=False))
        
        # Print summary
        summary = analyzer.generate_summary(results)
        print("\nAnalysis Summary:")
        print("=" * 30)
        for key, value in summary.items():
            print(f"{key}: {value}")
        
        # Save results to CSV
        output_file = csv_file.replace('.csv', '_analysis_results.csv')
        results.to_csv(output_file, index=False)
        print(f"\nResults saved to: {output_file}")
        
        return 0
        
    except Exception as e:
        print(f"Error analyzing IPDR data: {e}")
        return 1


if __name__ == "__main__":
    exit(main())
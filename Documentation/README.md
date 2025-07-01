# Data Engineer Home Assessment

This repository contains the complete solution for the Data Engineer Home Assessment, implementing both Problem Set-1 (timestamp difference calculation) and Problem Set-2 (IPDR call analysis).

## Overview

### Problem Set-1: Social Media Timestamp Calculator
- **Task A**: Calculate absolute time differences between social media posts with timezone support
- **Task B**: REST API service providing JSON responses
- **Task C**: Containerized multi-node deployment with Docker

### Problem Set-2: IPDR Call Analysis
- Analyzes IP Detail Records (IPDR) to identify and classify VoIP calls
- Distinguishes between audio and video calls based on bitrate analysis
- Processes telecommunications data for billing and usage insights

## Architecture

```
├── Problem_Set_1/
│   ├── Task_A/
│   │   ├── task_a.py              # Core timestamp difference calculator
│   │   └── test_input.txt         # Sample test data
│   ├── Task_B/
│   │   └── api_service.py         # Flask REST API service
│   └── Task_C/
│       ├── Dockerfile             # Container definition
│       ├── docker-compose.yml     # Multi-node orchestration
│       └── nginx.conf            # Load balancer configuration
├── Problem_Set_2/
│   ├── ipdr_analyzer.py           # IPDR call analysis engine
│   ├── ipdr.csv                   # Sample IPDR data
│   └── ipdr_analysis_results.csv  # Analysis results
├── Deployment/
│   └── requirements.txt           # Python dependencies
└── Documentation/
    └── README.md                  # This file
```

## Quick Start

### Prerequisites
- Python 3.12+
- Docker and Docker Compose
- curl (for testing)

### Local Development

1. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate
```

2. Install dependencies:
```bash
pip install -r Deployment/requirements.txt
```

3. Test Task A directly:
```bash
cd Problem_Set_1/Task_A
python3 task_a.py test_input.txt
```

4. Run API service:
```bash
cd Problem_Set_1/Task_B
python3 api_service.py
```

5. Analyze IPDR data:
```bash
cd Problem_Set_2
python3 ipdr_analyzer.py ipdr.csv
```

### Docker Deployment

1. Build and run multi-node setup:
```bash
cd Problem_Set_1/Task_C
docker-compose up --build
```

2. Access services:
- Node 1: http://localhost:5000
- Node 2: http://localhost:5001
- Load Balancer: http://localhost

## API Usage

### Problem Set-1 Endpoints

#### POST /calculate
Calculate timestamp differences (Task B format)

```bash
curl -X POST http://localhost:5000/calculate \
  -H "Content-Type: text/plain" \
  -d "2
Sun 10 May 2015 13:54:36 -0700
Sun 10 May 2015 13:54:36 -0000
Sat 02 May 2015 19:54:36 +0530
Fri 01 May 2015 13:54:36 -0000"
```

Response: `["25200","88200"]`

#### POST /calculate-with-node
Calculate with node information (Task C format)

```bash
curl -X POST http://localhost:5000/calculate-with-node \
  -H "Content-Type: text/plain" \
  -d "2
Sun 10 May 2015 13:54:36 -0700
Sun 10 May 2015 13:54:36 -0000
Sat 02 May 2015 19:54:36 +0530
Fri 01 May 2015 13:54:36 -0000"
```

Response: `{"id":"node-1","result":["25200","88200"]}`

#### GET /health
Health check endpoint

Response: `{"status":"healthy","node_id":"node-1"}`

## IPDR Analysis Details

### Algorithm Overview

1. **Data Preparation**: Parse timestamps and apply 10-minute idle time exclusion
2. **Call Identification**: Group consecutive FDRs by MSISDN and domain
3. **Call Processing**: Calculate duration, volume, and bitrate for each call
4. **Classification**: Categorize calls as audio (≤200 kbps) or video (>200 kbps)
5. **Filtering**: Remove calls with bitrate < 10 kbps

### Input Format
CSV with columns: `starttime,endtime,msisdn,ulvolume,dlvolume,domain`

### Output Format
```
msisdn,domain,duration_sec,fdr_count,kbps,is_audio,is_video
1,app1,602,2,2219.59,False,True
2,app1,247,1,619.21,False,True
```

### Key Metrics
- **Duration**: Call time in seconds (highest end time - lowest start time)
- **FDR Count**: Number of detail records comprising a single call
- **Bitrate**: Calculated as (total_volume_kb * 8) / (duration_sec / 3600)
- **Classification**: Audio if ≤200 kbps, Video if >200 kbps

## Docker Configuration

### Multi-Node Setup
The application runs in a containerized environment with:
- **2 API service nodes** (ports 5000, 5001)
- **Nginx load balancer** (port 80)
- **Health checks** for all services
- **Automatic restart policies**

### Node Identification
Each container instance includes a unique `NODE_ID` environment variable that appears in API responses, enabling request tracing across the distributed system.

## Testing

### Unit Testing Commands
```bash
# Test timestamp calculator
cd Problem_Set_1/Task_A && python3 task_a.py test_input.txt

# Test API endpoints
cd Problem_Set_1/Task_B && curl -X POST http://localhost:5000/calculate -H "Content-Type: text/plain" -d "$(cat ../Task_A/test_input.txt)"

# Test IPDR analysis
cd Problem_Set_2 && python3 ipdr_analyzer.py ipdr.csv

# Test Docker deployment
cd Problem_Set_1/Task_C && docker-compose up --build
curl http://localhost/health
```

### Sample Data
- `Problem_Set_1/Task_A/test_input.txt`: Sample timestamp data for Problem Set-1
- `Problem_Set_2/ipdr.csv`: Sample IPDR data for Problem Set-2

## Performance Considerations

### IPDR Processing
- Efficient pandas operations for large datasets
- Memory-optimized data type conversions
- Chunked processing capability for very large files
- Temporal indexing for fast time-based queries

### API Service
- Flask development server (production deployment requires WSGI server)
- Stateless design enabling horizontal scaling
- Health check endpoints for monitoring
- Error handling and input validation

## Error Handling

### Common Issues
1. **Invalid timestamp format**: Ensure timestamps follow "Day dd Mon yyyy hh:mm:ss +xxxx" format
2. **IPDR date parsing**: CSV datetime format must be "YYYY-MM-DD HH:MM:SS"
3. **Missing dependencies**: Install all requirements via `pip install -r Deployment/requirements.txt`
4. **Docker port conflicts**: Ensure ports 5000, 5001, and 80 are available

### Troubleshooting
- Check container logs: `docker-compose logs <service_name>`
- Verify API health: `curl http://localhost:5000/health`
- Test data format: Validate CSV structure matches expected schema

## Development Notes

### Code Structure
- **Problem_Set_1/Task_A/task_a.py**: Pure Python implementation with no external dependencies
- **Problem_Set_1/Task_B/api_service.py**: Flask-based REST API with error handling
- **Problem_Set_2/ipdr_analyzer.py**: Pandas-based data processing with statistical analysis
- **Modular design**: Each component can be used independently

### Security Features
- Non-root container user
- Input validation and sanitization
- Error message sanitization
- Health check endpoints for monitoring

## Future Enhancements

1. **Database integration** for persistent IPDR storage
2. **Authentication and authorization** for API endpoints
3. **Batch processing** for large IPDR datasets
4. **Real-time streaming** analysis capabilities
5. **Monitoring and alerting** integration
6. **Performance optimization** with caching layers
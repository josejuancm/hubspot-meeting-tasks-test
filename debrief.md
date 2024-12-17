# Code Review Debrief

## Code Quality & Readability
- Extract the API interaction logic into separate service classes:
  - `HubSpotMeetingService`
  - `HubSpotContactService`
  - Better organization of API calls
  - Reduce worker file size

- Implement TypeScript for:
  - Type safety
  - Complex HubSpot API responses

- Create constants or take the values from the environment variables for numbers and add the reason for those values:
  - 4 retries
  - 100 batch size
  - 9900 pagination limit
  - Reusable configuration

## Project Architecture
- Implement proper error handling and monitoring:
  - Structured logging
  - Replace console.logs (added for debugging purposes)
  - Implement a DAO and DTO classes for incoming and outgoing data

- Add queue system for:
  - Better job processing
  - Retry mechanisms
  - Data pulling process monitoring

## Code Performance
- Consider incremental updates:
  - Track changes at granular level
  - Replace full modified records pull within time range
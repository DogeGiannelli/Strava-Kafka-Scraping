# Near real-time scraping from Strava.com using Selenium and Kafka

***Part of the Data Management project | UniMiB***

The purpose of the project is to create a collection of Strava activities and make them available according to user's sport preference through Kafka (possible topics are: Running, Cycling, Water sports and Other).

![Copy of Add a subheading_page-0001 (2)](https://user-images.githubusercontent.com/84336749/228887842-c437a6d2-07cd-4b50-b17a-07682107c881.png)

**Note:** to avoid the Too Many Requests status code, there's random time sleep after the scraping of each activity. It avoids the error, but increases the time needed for each ID; to keep scraping in real-time it is possible to change the difference between IDs from 1 to 100, like in *scraping_producer.py.*

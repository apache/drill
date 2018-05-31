---
title: "Use Postman to Run SQL Queries on Drill Data Sources"
date: 2018-05-31 01:30:24 UTC
parent: "REST API"
---

You can run Drill queries from Postman, a Chrome browser extension and HTTP client for testing web services. To run Drill queries from Postman, install the Postman extension and then configure Postman to connect to a Drillbit and run queries.  

Complete the following steps to install and configure Postman to run Drill queries:  

1-To install the Postman Chrome browser extension, go to [https://chrome.google.com/webstore/detail/postman/fhbjgbiflinjbdggehcddcbncdddomop?hl=en](https://chrome.google.com/webstore/detail/postman/fhbjgbiflinjbdggehcddcbncdddomop?hl=en) and then click **+ ADD TO CHROME**.  
2-Click ![Postman Icon](https://i.imgur.com/a4pg98J.png) to open the application. You can go to [chrome://apps/](chrome://apps/) to find the Postman icon if you do not see it.   
3-In a new tab on the Builder page, set the type to **POST**, and enter the request URL as  `http://<drillbit-hostname>:8047/query.json`.  
  
![Request URL](https://i.imgur.com/slNIu8p.png)  
  
4-Select the **Headers** tab and then enter **Content-Type** as the key and **application/json** as the value. Add another entry with **User-Name** as the key and **mapr** as the value.  
  
![Headers](https://i.imgur.com/mreqm7S.png)  

5-Select the **Body** tab and then select the radio button labeled **raw**. A new drop-down list appears next to the radio buttons. Select **JSON** from the drop-down list.  

![Body](https://i.imgur.com/eaVfmve.png)  
  
6-In the body box, enter the request body in JSON format. For example, a test.csv file resides under the /tmp folder in the `dfs.tmp` schema. To test your configuration, you can enter the following query on the test.csv file: 
 
       {
       "queryType": "SQL",
       "query": "select * from `dfs.tmp`.`test.csv`"
       }

![Query](https://i.imgur.com/qnDENpV.png)  
  
7-Click **Send** to submit the query. The Postman displays the query results.  

![Query results](https://i.imgur.com/gLxMJjL.png)






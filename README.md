# SQL Practice Project in Baraa's full SQL course

## Purpose:
This query documents my writing of the practice project in the full SQL course offered by 'Data with Baraa'. 

## System & Tools:
***Google Cloud BigQuery***  
***Google Gemini AI***  
:wave: Although Baraa uses SQL Server for his whole instruction, I opted for Google Cloud to do this practise to understand mainstream DBMS better (BigQuery is obviously an overkill for this project though:grinning:).  Using Gemini along the way also showed me inspiring ways to build my query.

## Contents:
The project was broken down into **three** stages:
   - Data Warehousing
   - Exploratory Data Analysis (EDA)
   - 'Advanced' Data Analytics  
   The project archetecture is shown in the following picture:
   <img width="1544" height="912" alt="data_architecture" src="https://github.com/user-attachments/assets/27d426cc-9200-4868-aa85-58ed852b8e51" />

1. Data Warehousing Stage  
  This is the most bulky part of the project. Corresponding Queries are from '**0.Bronze_Layer_Insert_Data.sql**' to '**7.Gold_Layer_Quality_Check.sql**' :eyes:.  
  The raw data came in csv. files downloaded from Baraa's website, with the following relationship:
  <img width="1522" height="861" alt="data_integration" src="https://github.com/user-attachments/assets/11e30aea-c793-4f5d-8ce6-90739f9ab33c" />

  According to the **Separation of Concern (SOC)** principle, the project utilise three layers/schema in the ETL process:
  <img width="1195" height="672" alt="Screenshot 2025-11-13 at 12 56 06" src="https://github.com/user-attachments/assets/e94251f5-5c0b-4742-ac16-7f07a0a86801" />    
  
  The whole **Extraction/Transformation/Load(ETL)** underwent the process with methods and techniques shown in the following picture:
  <img width="1839" height="1759" alt="ETL" src="https://github.com/user-attachments/assets/e8adb52d-135c-4768-a2c3-74e8ea631b40" />   
  
  The resulting gold layer ready for data analysis uses **Star Schema** and is like the following:
  <img width="1500" height="667" alt="data_model" src="https://github.com/user-attachments/assets/55664fcf-0dfc-44a8-9917-8532572b9593" />  
  With this, the project goes on to its analysis stage.

2. Exploratory Data Analysis Stage  
  This stage corresponds to the query '**8. Exploratory_Data_Analysis(EDA).sql**' :eyes:.  
  The content of the analysis is shown together in the next note.

3. 'Advanced' Data Analytics Stage  
  This stage corresponds to the query '**9. Advanced_Data_Analytics.sql**' :eyes:.  
  The content, together with the EDA, is shown in the following image:
  <img width="1999" height="897" alt="data_analysis" src="https://github.com/user-attachments/assets/5a79a646-ed1c-4187-871d-2975197edd9d" />  

> [!NOTE]
This project is a wrap-up of the whole 30 hour SQL course from Baraa. Due to the constraints of all similar projects, it only covers part of the contents taught in the course. However, it serves as an important start for my continuing practise and exploration in the data world using this essential tool of SQL.  
I would like to thank Baraa for his amazing course and guidance. @DataWithBaraa  

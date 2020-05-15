
# RFM in snowSQL

### This repository contain 3 SQL file (snowSQL)

- mapping_segment: Simple snowSQL file to create a mapping table with the 125 possible combinations of the RFM scores, the name of the corresponding segment and the ordinal number of that segment. The combination are made from https://docs.exponea.com/docs/rfm-segmentation
- rfm_table: Template to create a table with RFM scores, raw values and segments. The template will calculate the daily RFM based on a few criteria, for one year and take the last row for customer which corresponds to the RFM of today. It uses a few temporary table (which is what I usually use when I create template before delivering)
- rfm_view: Template to create a view with RFM scores. The core of the code is the same that the table, but the view is one longer query with no temporary or transitional tables. The view is particularly useful when you need an automate update. To automate the table of RFM, the engineering of the warehouse need to give you special permission in most common case, while the view is automatic up-date every time that a table involved change. It is much slower to query because every time need calculate it from the scratch. But if connected to other tools like Tableau, is possible to set up a frequency of update and visualise the fresh version

### Criteria chosen

- F (sum of transactions), M (sum of revenue) are calculated in the previous 3 months starting from last day
- R until 6 months. This means that customer inactive for more than 6 months are out of the RFM calculation
- At beginning there is the option to choose the country of the customer as I used to differentiate the RFM for the different core markets

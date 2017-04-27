library(xlsx)
library(dplyr)
library(RODBC)
library(stringr)
library(lubridate)

countryCode <- read.csv(file = "F:\\Mobile - Rabbids\\data tracking\\Data iOS - Houston\\countryCode.csv",stringsAsFactors = F) %>% tbl_df()


ch <- odbcConnect("rcr_redshift")
hdata <- sqlQuery(channel = ch
                  ,query = "
                  SELECT CASE WHEN store = 'APPSTORE' THEN 'iOS' ELSE 'Android' END as platform
		                ,CASE WHEN store = 'APPSTORE' THEN 'e46e616e-3347-4891-b86e-e3dd9ec4eb29' ELSE '18706323-c903-4323-83a9-0d570a588525' END as appid
                  ,ubi_transaction_id AS id
                  ,store_transaction_id AS transaction_id
                  ,cntry_territory_ft_time AS purchase_date
                  ,item AS item_id
                  ,country AS countryCode
                  ,device_id
                  ,CASE WHEN local_currency_code = '' AND country = 'NOR' THEN 'NOK'
                  WHEN local_currency_code = '' AND country = 'SAU' THEN 'SAR'
                  ELSE local_currency_code END AS currency
                  ,local_price AS price_local
                  ,CASE WHEN local_currency_amount IS NULL THEN CAST(regexp_replace(regexp_substr(local_price, '[0-9]+(\\.|,)[0-9]+'), ',', '\\.') AS DOUBLE PRECISION)
                  ELSE local_currency_amount END AS price_local1
                  FROM dp_rabbids_cr.rcr_houston
                  WHERE local_price != '[PROMO_CODE]'
                  "
                  ,stringsAsFactors = F) %>% tbl_df()

odbcClose(channel = ch)

t <- hdata %>%
    left_join(countryCode, by = c("countrycode" = "country3code")) %>%
    select(platform:item_id, countrycode = countrycode.y, device_id:price_local1)

# timezone will be automatically changed when write time into Excel file
# timezone has to be force to set as GMT
Sys.setenv(TZ="")
t$purchase_date <- force_tz(t$purchase_date, tzone = 'GMT')


file_path_xlsx <- paste("F:\\Mobile - Rabbids\\data tracking\\Data iOS - Houston\\"
                   ,format(Sys.Date(), format="%Y.%m.%d"), "_", "houston_transaction_redshift.xlsx", collapse = "", sep = "")

file_path_csv <- paste("F:\\Mobile - Rabbids\\data tracking\\Data iOS - Houston\\"
                   ,format(Sys.Date(), format="%Y.%m.%d"), "_", "houston_transaction_redshift.csv", collapse = "", sep = "")

write.xlsx(x = as.data.frame(t), file = file_path_xlsx, row.names = F, showNA = FALSE)

write.csv(x = as.data.frame(t), file = file_path_csv)

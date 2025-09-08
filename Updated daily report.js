const axios = require('axios').default;
const fs = require('fs');

module.exports = async function (context, myTimer) {
    var timeStamp = new Date(GetFullDateTime()).toISOString();
    
    context.log(timeStamp)

    if (myTimer.isPastDue)
    {
    }

    let csvText = '';

    let userData = {};
    var dateToday = DateToday();
    var currentDate = new Date(GetFullDateTime());

    let currYear = `${currentDate.getFullYear()}`;
    let thisMonthDatetime2 = `${currYear}-${currentDate.getMonth() + 1}-01 00:00:00.0000000`;

    if (currentDate.getMonth() == 0)
    {
        currYear = currYear - 1;
        currMonth = 12;
    }
    else
    {
        currMonth = `${currentDate.getMonth()}`
    }

    let lastMonthDatetime2 = `${currYear}-${currMonth}-01 00:00:00.0000000`;

    userData.requestBotStats = {};
    userData.requestBotStats.thisMonthDatetime2 = thisMonthDatetime2;
    userData.requestBotStats.lastMonthDatetime2 = lastMonthDatetime2;


    context.log('GetFullDateTime() = ', GetFullDateTime());   
    
    let sqlQueryList = [];
    let queryText;
    let sqlQueryResult;

    //DEV1 QUERIES
    queryText = `
            select AVG(CAST(SCORE as float)) as 'CARDS_APP_NET_PROMOTER_SURVEY_AVERAGE_SCORE'
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO'
            
            select COUNT(*) as 'CARDS_APP_NET_PROMOTER_SURVEY_PARTICIPANTS'
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO'
            
            SELECT 
            (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO')) * 100 as CARDS_APP_NPS_PROMOTERS_PERCENTAGE
            FROM [dbo].[NET_PROMOTER_SURVEY] 
            where SCORE >= 9 and PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO'

            SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO')) * 100 as CARDS_APP_NPS_NEUTRAL_PERCENTAGE
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where (SCORE = 7 or SCORE = 8) and PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO'

            SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO')) * 100 as CARDS_APP_NPS_DETRACTOR_PERCENTAGE 
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where SCORE < 7 and PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO'

            SELECT CAST((
            (
                (SUM(CASE WHEN SCORE BETWEEN 9 AND 10 THEN 1 ELSE 0 END) * 1.0) -
                SUM(CASE WHEN SCORE BETWEEN 0 AND 6 THEN 1 ELSE 0 END) 
            )
            /COUNT(*) * 100) AS int) as CARDS_APP_NET_PROMOTER_SCORE 
            FROM NET_PROMOTER_SURVEY
            WHERE SCORE IS NOT NULL and PROGRAM_SOURCE = 'CARD APPLICATION SUBMIT VIDEO'
            
            -----NPS REGISTRATION 2

            select AVG(CAST(SCORE as float)) as 'REGISTRATION_2_PROMOTER_SURVEY_AVERAGE_SCORE'
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where PROGRAM_SOURCE = 'REGISTRATION 2'
            
            select COUNT(*) as 'REGISTRATION_2_NET_PROMOTER_SURVEY_PARTICIPANTS'
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where PROGRAM_SOURCE = 'REGISTRATION 2'
            
            SELECT 
            (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'REGISTRATION 2')) * 100 as REGISTRATION_2_NPS_PROMOTERS_PERCENTAGE
            FROM [dbo].[NET_PROMOTER_SURVEY] 
            where SCORE >= 9 and PROGRAM_SOURCE = 'REGISTRATION 2'

            SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'REGISTRATION 2')) * 100 as REGISTRATION_2_NPS_NEUTRAL_PERCENTAGE
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where (SCORE = 7 or SCORE = 8) and PROGRAM_SOURCE = 'REGISTRATION 2'

            SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'REGISTRATION 2')) * 100 as REGISTRATION_2_NPS_DETRACTOR_PERCENTAGE 
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where SCORE < 7 and PROGRAM_SOURCE = 'REGISTRATION 2'

            SELECT CAST((
            (
                (SUM(CASE WHEN SCORE BETWEEN 9 AND 10 THEN 1 ELSE 0 END) * 1.0) -
                SUM(CASE WHEN SCORE BETWEEN 0 AND 6 THEN 1 ELSE 0 END) 
            )
            /COUNT(*) * 100) AS int) as REGISTRATION_2_NET_PROMOTER_SCORE 
            FROM NET_PROMOTER_SURVEY
            WHERE SCORE IS NOT NULL and PROGRAM_SOURCE = 'REGISTRATION 2'

            -----------NPS REGISTRATION 2 END

 
            `;

    context.log(`---query1a---`);
    context.log(queryText);
    context.log(`---query1a---`);

    sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'dev1-SEA');
    if (sqlQueryResult != 'fail') 
    {
        sqlQueryList.push(sqlQueryResult);
        context.log(`query1a SUCCESS`);
    } else
    {
        context.log(`query1a FAILED`);
    }

    context.log(`---query1result---`);
    context.log(sqlQueryResult);
    context.log(`---query1result---`);

    queryText = `          

            -----NPS REGISTRATION 3

            select AVG(CAST(SCORE as float)) as 'REGISTRATION_3_PROMOTER_SURVEY_AVERAGE_SCORE'
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where PROGRAM_SOURCE = 'REGISTRATION 3'
            
            select COUNT(*) as 'REGISTRATION_3_NET_PROMOTER_SURVEY_PARTICIPANTS'
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where PROGRAM_SOURCE = 'REGISTRATION 3'
            
            SELECT 
            (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'REGISTRATION 3')) * 100 as REGISTRATION_3_NPS_PROMOTERS_PERCENTAGE
            FROM [dbo].[NET_PROMOTER_SURVEY] 
            where SCORE >= 9 and PROGRAM_SOURCE = 'REGISTRATION 3'

            SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'REGISTRATION 3')) * 100 as REGISTRATION_3_NPS_NEUTRAL_PERCENTAGE
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where (SCORE = 7 or SCORE = 8) and PROGRAM_SOURCE = 'REGISTRATION 3'

            SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY] WHERE PROGRAM_SOURCE = 'REGISTRATION 3')) * 100 as REGISTRATION_3_NPS_DETRACTOR_PERCENTAGE 
            FROM [dbo].[NET_PROMOTER_SURVEY]
            where SCORE < 7 and PROGRAM_SOURCE = 'REGISTRATION 3'

            SELECT CAST((
            (
                (SUM(CASE WHEN SCORE BETWEEN 9 AND 10 THEN 1 ELSE 0 END) * 1.0) -
                SUM(CASE WHEN SCORE BETWEEN 0 AND 6 THEN 1 ELSE 0 END) 
            )
            /COUNT(*) * 100) AS int) as REGISTRATION_3_NET_PROMOTER_SCORE 
            FROM NET_PROMOTER_SURVEY
            WHERE SCORE IS NOT NULL and PROGRAM_SOURCE = 'REGISTRATION 3'

            -----------NPS REGISTRATION 3 END
            `;

    context.log(`---query1b---`);
    context.log(queryText);
    context.log(`---query1b---`);
    sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'dev1-SEA');
    if (sqlQueryResult != 'fail') 
    {
        sqlQueryList.push(sqlQueryResult);
        context.log(`query1b SUCCESS`);
    } else
    {
        context.log(`query1b FAILED`);
    }

    context.log(`---query1result---`);
    context.log(sqlQueryResult);
    context.log(`---query1result---`);

    //PROD US QUERIES
   queryText = 
    `
        SELECT COUNT( DISTINCT CUST_NBR)  as 'REGISTERED_USERS'
        FROM [dbo].[CUSTOMER]
        where fb_psid is not null and FB_PSID != '' 

        SELECT COUNT( DISTINCT CUST_NBR)  as 'TOTAL_CARDHOLDERS'
        FROM [dbo].[CUSTOMER]

        SELECT COUNT(*)  as 'TOTAL_CARDS_REGISTERED'
        FROM [dbo].[CUSTOMER]
        where fb_psid is not null and FB_PSID != '' 

        SELECT COUNT( DISTINCT CARD_UCRN_NBR)  as 'TOTAL_UNIQUE_UCRNs'
        FROM [dbo].[CUSTOMER]

        SELECT COUNT( DISTINCT FB_PSID) as 'UNIQUE_INTERACTIONS'
        FROM [dbo].[BOT_INTERACTION_LOGS_NEW] 

        select count (*) as NC_CARDS_COUNT
        from [dbo].[CUSTOMER]          
        where CARD_BLK_CODE = 'NC'

        ----------------

        SELECT COUNT (*) as DISPUTES_COUNT_TOTAL
        FROM [dbo].[TRANSACTION_DISPUTES]

        SELECT COUNT (*) as DISPUTES_COUNT_APPROVED
        FROM [dbo].[TRANSACTION_DISPUTES]
        where STATUS = 'APPROVED'

        SELECT COUNT (*) as DISPUTES_COUNT_DECLINED
        FROM [dbo].[TRANSACTION_DISPUTES]
        where STATUS = 'DECLINED'
        
        ----------------

        SELECT COUNT (*) as CREDIT_LIMIT_INCREASE_TOTAL
        FROM [dbo].[LIMIT_INCREASE]

        SELECT COUNT (*) as CREDIT_LIMIT_INCREASE_APPROVED
        FROM [dbo].[LIMIT_INCREASE]
        where STATUS = 'APPROVED'

        SELECT COUNT (*) as CREDIT_LIMIT_INCREASE_DECLINED
        FROM [dbo].[LIMIT_INCREASE]
        where STATUS = 'DECLINED'

        ----------------

        SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'BALCON_LOANS_AMOUNT_APPROVED_TOTAL_PHP'
        FROM  [dbo].[BALANCE_CONVERSION]
        where STATUS = 'APPROVED'

        SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'BALCON_LOANS_AMOUNT_APPROVED_COLLECTIONS_PHP'
        FROM  [dbo].[BALANCE_CONVERSION]
        where STATUS = 'APPROVED' and BALCON_SOURCE = 'COLLECTIONS'

        SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'BALCON_LOANS_AMOUNT_APPROVED_MARKETING_PHP'
        FROM  [dbo].[BALANCE_CONVERSION]
        where STATUS = 'APPROVED' and BALCON_SOURCE = 'MARKETING'

        select count(*)   as 'BALCON_APPLICATIONS_COUNT_TOTAL'
        from [dbo].[BALANCE_CONVERSION] 

        select count(*)   as 'BALCON_APPLICATIONS_COUNT_COLLECTIONS'
        from [dbo].[BALANCE_CONVERSION] 
        where BALCON_SOURCE = 'COLLECTIONS'

        select count(*)   as 'BALCON_APPLICATIONS_COUNT_MARKETING'
        from [dbo].[BALANCE_CONVERSION] 
        where BALCON_SOURCE = 'MARKETING'

        select count(*)   as 'BALCON_APPLICATIONS_ALL_APPROVED'
        from [dbo].[BALANCE_CONVERSION] 
        where STATUS = 'APPROVED'

        select count(*)   as 'BALCON_APPLICATIONS_ALL_DECLINED'
        from [dbo].[BALANCE_CONVERSION] 
        where STATUS = 'DECLINED'

        ----------------
        SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'BALCON_AMOUNT_THIS_MONTH'
        FROM  [dbo].[BALANCE_CONVERSION]
        where (STATUS = 'PENDING' or STATUS = 'APPROVED')
        and CAST (CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}'
        
        SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'BALCON_AMOUNT_LAST_MONTH'
        FROM  [dbo].[BALANCE_CONVERSION]
        where STATUS = 'APPROVED' and
        CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
        CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
        
        SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'BALCON_LOANS_AMOUNT_TOTAL_PHP'
        FROM  [dbo].[BALANCE_CONVERSION] 

        select count(*)   as 'BALCON_APPLICATIONS_TOTAL'
        from [dbo].[BALANCE_CONVERSION]

        ----------------


    `;

    context.log(`---query2a---`);
    context.log(queryText);
    context.log(`---query2a---`);

    sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
    if (sqlQueryResult != 'fail') 
    {
        sqlQueryList.push(sqlQueryResult);
        context.log(`sqlQuery2a SUCCESS`);
    } else
    {
        context.log(`sqlQuery2a FAILED`);
    }

    context.log(`---query2aresult---`);
    context.log(sqlQueryResult);
    context.log(`---query2aresult---`);




    //-----2B


    queryText = 
        `
        select count(*)   as 'BNPL_LOANS_APPLICATIONS_TOTAL'
        from [dbo].[CONVERT_TO_INSTALLMENT]
        where ( CTI_INTEREST_PROFILE = '3' or CTI_INTEREST_PROFILE = '4' ) and  CONVERT(VARCHAR, INSTALLMENT_MATRIX) =   '3 months'

        select count(*)   as 'BNPL_LOANS_APPLICATIONS_APPROVED'
        from [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'APPROVED' and  ( CTI_INTEREST_PROFILE = '3' or CTI_INTEREST_PROFILE = '4' ) and  CONVERT(VARCHAR, INSTALLMENT_MATRIX) =   '3 months'

        select count(*)   as 'BNPL_LOANS_APPLICATIONS_DECLINED'
        from [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'DECLINED' and  ( CTI_INTEREST_PROFILE = '3' or CTI_INTEREST_PROFILE = '4' ) and  CONVERT(VARCHAR, INSTALLMENT_MATRIX) =   '3 months'

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BNPL_LOANS_AMOUNT_PHP'
        FROM [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'APPROVED' and  ( CTI_INTEREST_PROFILE = '3' or CTI_INTEREST_PROFILE = '4' ) and  CONVERT(VARCHAR, INSTALLMENT_MATRIX) =   '3 months'

        ----------------

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'APPROVED_CTI_LOANS_AMOUNT_PHP'
        FROM [dbo].[CONVERT_TO_INSTALLMENT]
        where (STATUS = 'APPROVED' or STATUS = 'BIZFUSE APPROVED') and  ( CTI_INTEREST_PROFILE != '3' and CTI_INTEREST_PROFILE != '4' and CONVERT(VARCHAR, INSTALLMENT_MATRIX) != '3 months' ) 

        SELECT COUNT(*) * 250 as 'CTI_TOTAL_PROCESSING_FEE_COLLECTED_PHP'
        FROM [dbo].[CONVERT_TO_INSTALLMENT]
        where (STATUS = 'APPROVED' or STATUS = 'BIZFUSE APPROVED') and [PROCESSING_FEE_CONSENT] = 'YES'

        select count(*)   as 'CTI_LOANS_APPLICATIONS_TOTAL'
        from [dbo].[CONVERT_TO_INSTALLMENT] 
        where ( CTI_INTEREST_PROFILE != '3' and CTI_INTEREST_PROFILE != '4' and CONVERT(VARCHAR, INSTALLMENT_MATRIX) != '3 months' ) 

        select count(*)   as 'CTI_LOANS_APPLICATIONS_APPROVED'
        from [dbo].[CONVERT_TO_INSTALLMENT]
        where (STATUS = 'APPROVED' or STATUS = 'BIZFUSE APPROVED') and  ( CTI_INTEREST_PROFILE != '3' and CTI_INTEREST_PROFILE != '4' and CONVERT(VARCHAR, INSTALLMENT_MATRIX) != '3 months' ) 

        select count(*)   as 'CTI_LOANS_APPLICATIONS_DECLINED'
        from [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'DECLINED' and  ( CTI_INTEREST_PROFILE != '3' and CTI_INTEREST_PROFILE != '4' and CONVERT(VARCHAR, INSTALLMENT_MATRIX) != '3 months' ) 

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as money)) as 'CTI_AMOUNT_THIS_MONTH'
        FROM  [dbo].[CONVERT_TO_INSTALLMENT]
        where (STATUS != 'DECLINED') and
        CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}' 

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as money)) as 'CTI_AMOUNT_LAST_MONTH'
        FROM  [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS != 'DECLINED' and
        CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
        CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
        
        ------------------------------

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BNPL_LOANS_AMOUNT_TOTAL_APPROVED_PHP'
        FROM [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'APPROVED'
        and APPLICATION_TYPE = 'BNPL' and  CTI_INTEREST_PROFILE = '3' 
        and CONVERT(VARCHAR, INSTALLMENT_MATRIX) = '3 months'

        SELECT COUNT(*) * 500 as 'BNPL_TOTAL_PROCESSING_FEE_COLLECTED_PHP'
        FROM [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'APPROVED'
        and APPLICATION_TYPE = 'BNPL'
        and PROCESSING_FEE_CONSENT = 'YES' and
        CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
        CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as money)) as 'BNPL_AMOUNT_THIS_MONTH'
        FROM  [dbo].[CONVERT_TO_INSTALLMENT]
        where (STATUS = 'PENDING' or STATUS = 'APPROVED')
        and APPLICATION_TYPE = 'BNPL' and
        CAST (CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}'

        SELECT SUM(CAST(replace(AMOUNT, ',', '') as money)) as 'BNPL_AMOUNT_LAST_MONTH'
        FROM  [dbo].[CONVERT_TO_INSTALLMENT]
        where STATUS = 'APPROVED'
        and APPLICATION_TYPE = 'BNPL' and
        CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
        CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
        
        ----------------


        `;

    context.log(`---query2b---`);
    context.log(queryText);
    context.log(`---query2b---`);

    sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
    if (sqlQueryResult != 'fail') 
    {
        sqlQueryList.push(sqlQueryResult);
        context.log(`sqlQuery2b SUCCESS`);
    } else
    {
        context.log(`sqlQuery2b FAILED`);
    }

    context.log(`---query2bresult---`);
    context.log(sqlQueryResult);
    context.log(`---query2bresult---`);

    

    ///--2c

    queryText = 
    `

    SELECT SUM(CAST(TOTAL_CASH_REDEEMED as float))as 'APPROVED_REWARDS_REDEMPTION_AMOUNT_PHP'
        FROM [dbo].[REWARDS_PTS_CONVERSION]
        where STATUS = 'APPROVED'

        SELECT SUM(CAST(TOTAL_PTS_REDEEMED as float))as 'TOTAL_REWARDS_POINTS_CONSUMED'
        FROM [dbo].[REWARDS_PTS_CONVERSION]
        where STATUS = 'APPROVED'

        select count(*)   as 'REWARDS_APPLICATIONS_TOTAL'
        from [dbo].[REWARDS_PTS_CONVERSION]

        select count(*)   as 'REWARDS_APPLICATIONS_APPROVED'
        from [dbo].[REWARDS_PTS_CONVERSION]
        where STATUS = 'APPROVED'

        select count(*)   as 'REWARDS_APPLICATIONS_DECLINED'
        from [dbo].[REWARDS_PTS_CONVERSION]
        where STATUS = 'DECLINED'

        SELECT count(*) as FEEDBACK_GENERAL_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'General'

        SELECT count(*) as FEEDBACK_GENERAL_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'General' and STATUS = 'CLOSED'

        SELECT count(*) as ESOA_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'enroll esoa'

        SELECT count(*) as ESOA_TOTAL_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'enroll esoa' and STATUS = 'CLOSED'

        SELECT count(*) as FEEDBACK_RETAIL_INTEREST_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Retail Interest'

        SELECT count(*) as FEEDBACK_RETAIL_INTEREST_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Retail Interest' and STATUS = 'CLOSED'

        SELECT count(*) as FEEDBACK_CARD_DELIVERY_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Card Delivery'

        SELECT count(*) as FEEDBACK_CARD_DELIVERY_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Card Delivery' and STATUS = 'CLOSED'

        SELECT count(*) as FEEDBACK_STATEMENT_DELIVERY_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Statement Delivery'

        SELECT count(*) as FEEDBACK_STATEMENT_DELIVERY_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Statement Delivery' and STATUS = 'CLOSED'

        SELECT count(*) as FEEDBACK_STATEMENT_ENTRIES_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Statement Entries'

        SELECT count(*) as FEEDBACK_STATEMENT_ENTRIES_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Statement Entries' and STATUS = 'CLOSED'

        SELECT count(*) as LOST_BLOCK_CARD_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'lost/block card'

        SELECT count(*) as LOST_BLOCK_CARD_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'lost/block card' and STATUS = 'CLOSED'

        SELECT count(*) as HOW_TO_PAY_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'How to pay'

        SELECT count(*) as HOW_TO_PAY_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'How to pay' and STATUS = 'CLOSED'

        SELECT count(*) as PROMOS_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Promos'

        SELECT count(*) as PROMOS_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'Promos' and STATUS = 'CLOSED'

        SELECT count(*) as CARD_CANCELLATION_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'CARD CANCELLATION'

        SELECT count(*) as CARD_CANCELLATION_CLOSED_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'CARD CANCELLATION' and STATUS = 'CLOSED'

        SELECT count(*) as ANNUAL_FEE_WAIVER_V1_TOTAL
        FROM [dbo].[FEEDBACK]
        where SOURCE_DIALOG = 'ANNUAL FEE WAIVER'

        SELECT count(*) as ANNUAL_FEE_WAIVER_V2_TOTAL
        FROM [dbo].[ANNUAL_FEE_WAIVER_REQUESTS]
    `;

context.log(`---query2c---`);
context.log(queryText);
context.log(`---query2c---`);

sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
if (sqlQueryResult != 'fail') 
{
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery2c SUCCESS`);
} else
{
    context.log(`sqlQuery2c FAILED`);
}

context.log(`---query2aresult---`);
context.log(sqlQueryResult);
context.log(`---query2aresult---`);



//*START
    //PROD US QUERIES
    queryText = 
`
    SELECT count(*) as UPDATE_MOBILE_TOTAL
    FROM [dbo].[MOBILE_NUMBER_UPDATE_LIST]

    SELECT count(*) as UPDATE_EMAIL_TOTAL
    FROM  [dbo].[EMAIL_ADDRESS_UPDATE_LIST]

    SELECT SUM(PRESS_START_COUNT) as PRESS_START_COUNT
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]

    SELECT SUM(STATEMENT_CHECK_COUNT)  as STATEMENT_CHECK_COUNT
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_24_HOURS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -1))}' AND '${DateToday()}'

        SELECT count(*) as UNIQUE_INTERACTIONS_LAST_7_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -7))}' AND '${DateToday()}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_30_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -30))}' AND '${DateToday()}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_60_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -60))}' AND '${DateToday()}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_90_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -90))}' AND '${DateToday()}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_180_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -180))}' AND '${DateToday()}'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_24_HOURS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -1))}' AND '${DateToday()}'  and  REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_7_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -7))}' AND '${DateToday()}'  and  REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_30_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -30))}' AND '${DateToday()}'  and  REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_60_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -60))}' AND '${DateToday()}'  and  REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_90_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -90))}' AND '${DateToday()}'  and  REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_180_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime,  [DATE_LAST_INTERACTION], 120) BETWEEN '${formatDate(addDays(DateToday(), -180))}' AND '${DateToday()}'  and  REGISTERED = 'true'

    SELECT COUNT(*)  as 'TOTAL_SUPPLE_APPLICATIONS'
    FROM [dbo].[SUPPEMENTARY_CARD_APPLICATIONS]

    SELECT COUNT(*)  as 'TOTAL_APPROVED_SUPPLE_APPLICATIONS'
    FROM [dbo].[SUPPEMENTARY_CARD_APPLICATIONS]
    where STATUS = 'APPROVED'

    SELECT COUNT(*)  as 'TOTAL_DECLINED_SUPPLE_APPLICATIONS'
    FROM [dbo].[SUPPEMENTARY_CARD_APPLICATIONS]
    where STATUS = 'DECLINED'
    
    SELECT COUNT(*)  as 'TOTAL_NEW_SUPPLE_APPLICATIONS'
    FROM [dbo].[SUPPEMENTARY_CARD_APPLICATIONS]
    where STATUS like '%NEW%'

    SELECT COUNT(*)  as 'TOTAL_TC_CARDS'
    FROM [dbo].[CUSTOMER]
    where CARD_BLK_CODE = 'TC'

    SELECT COUNT(*)  as 'TOTAL_TT_CARDS'
    FROM [dbo].[CUSTOMER]
    where CARD_BLK_CODE = 'TT'

    select AVG(CAST(SCORE as float)) as 'NET_PROMOTER_SURVEY_AVERAGE_SCORE'
    FROM [dbo].[NET_PROMOTER_SURVEY]

    select COUNT(*) as 'NET_PROMOTER_SURVEY_PARTICIPANTS'
    FROM [dbo].[NET_PROMOTER_SURVEY]
    
    SELECT 
    (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY])) * 100 as NPS_PROMOTERS_PERCENTAGE
    FROM [dbo].[NET_PROMOTER_SURVEY]
    where SCORE >= 9

    SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY])) * 100 as NPS_NEUTRAL_PERCENTAGE
    FROM [dbo].[NET_PROMOTER_SURVEY]
    where SCORE = 7 or SCORE = 8

    SELECT  (Cast(count (*) as float)/ (SELECT count (*) as NPS_RESPONSES FROM [dbo].[NET_PROMOTER_SURVEY])) * 100 as NPS_DETRACTOR_PERCENTAGE 
    FROM [dbo].[NET_PROMOTER_SURVEY]
    where SCORE < 7

    SELECT CAST((
    (
        (SUM(CASE WHEN SCORE BETWEEN 9 AND 10 THEN 1 ELSE 0 END) * 1.0) -
        SUM(CASE WHEN SCORE BETWEEN 0 AND 6 THEN 1 ELSE 0 END) 
    )
    /COUNT(*) * 100) AS int) as NET_PROMOTER_SCORE 
    FROM NET_PROMOTER_SURVEY
    WHERE SCORE IS NOT NULL
`;

context.log(`---query3a---`);
context.log(queryText);
context.log(`---query3a---`);

sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
if (sqlQueryResult != 'fail') 
{
sqlQueryList.push(sqlQueryResult);
context.log(`sqlQuery3a SUCCESS`);
} else
{
context.log(`sqlQuery3a FAILED`);
}

context.log(`---query3aresult---`);
context.log(sqlQueryResult);
context.log(`---query3aresult---`);


//3B

queryText = 
`
    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'INSTACASH_AMOUNT_THIS_MONTH'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where (STATUS = 'PENDING' or STATUS = 'APPROVED') and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}' 

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'INSTACASH_AMOUNT_LAST_MONTH'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
    CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'INSTACASH_LOANS_AMOUNT_APPROVED_TOTAL_PHP'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED'

    SELECT COUNT(*) *250 as 'INSTACASH_TOTAL_PROCESSING_FEE_COLLECTED_PHP'
    FROM [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED' and [PROCESSING_FEE_CONSENT] = 'YES'

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'INSTACASH_LOANS_AMOUNT_TOTAL_PHP'
    FROM  [dbo].[INSTACASH_APPLICATIONS]

    select count(*)   as 'INSTACASH_APPLICATIONS_TOTAL'
    from [dbo].[INSTACASH_APPLICATIONS]

    select count(*)   as 'INSTACASH_APPLICATIONS_APPROVED'
    from [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED'

    select count(*)   as 'INSTACASH_APPLICATIONS_DECLINED'
    from [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'DECLINED'

    -------------------------------

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'INSTACASH_AMOUNT_THIS_MONTH_ESTA'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where (STATUS = 'PENDING' or STATUS = 'APPROVED') and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'INSTACASH_AMOUNT_LAST_MONTH_ESTA'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
    CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')


    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'INSTACASH_LOANS_AMOUNT_APPROVED_TOTAL_PHP_ESTA'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')


    SELECT COUNT(*) *250 as 'INSTACASH_TOTAL_PROCESSING_FEE_COLLECTED_PHP_ESTA'
    FROM [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED' and [PROCESSING_FEE_CONSENT] = 'YES'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')


    SELECT SUM(CAST(REPLACE(LOAN_AMOUNT, ',', '') AS FLOAT)) AS 'INSTACASH_LOANS_AMOUNT_TOTAL_PHP_ESTA'
    FROM [dbo].[INSTACASH_APPLICATIONS]
    WHERE (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(ISNULL(bot_used, ''))) = '')


    select count(*)   as 'INSTACASH_APPLICATIONS_TOTAL_ESTA'
    from [dbo].[INSTACASH_APPLICATIONS]
    WHERE (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')


    select count(*)   as 'INSTACASH_APPLICATIONS_APPROVED_ESTA'
    from [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')


    select count(*)   as 'INSTACASH_APPLICATIONS_DECLINED_ESTA'
    from [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'DECLINED'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    -------------------------------

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'INSTACASH_AMOUNT_THIS_MONTH_DXP'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where (STATUS = 'PENDING' or STATUS = 'APPROVED') and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}'
    and bot_used = 'DXP' 

    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as money)) as 'INSTACASH_AMOUNT_LAST_MONTH_DXP'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
    CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
    and bot_used = 'DXP' 


    SELECT SUM(CAST(replace(LOAN_AMOUNT, ',', '') as float)) as 'INSTACASH_LOANS_AMOUNT_APPROVED_TOTAL_PHP_DXP'
    FROM  [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED'
    and bot_used = 'DXP' 


    SELECT COUNT(*) *250 as 'INSTACASH_TOTAL_PROCESSING_FEE_COLLECTED_PHP_DXP'
    FROM [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED' and [PROCESSING_FEE_CONSENT] = 'YES'
    and bot_used = 'DXP'


    SELECT SUM(CAST(REPLACE(LOAN_AMOUNT, ',', '') AS FLOAT)) AS 'INSTACASH_LOANS_AMOUNT_TOTAL_PHP_DXP'
    FROM [dbo].[INSTACASH_APPLICATIONS]
    WHERE bot_used = 'DXP'


    select count(*)   as 'INSTACASH_APPLICATIONS_TOTAL_DXP'
    from [dbo].[INSTACASH_APPLICATIONS]
    WHERE bot_used = 'DXP' 


    select count(*)   as 'INSTACASH_APPLICATIONS_APPROVED_DXP'
    from [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'APPROVED'
    and bot_used = 'DXP'


    select count(*)   as 'INSTACASH_APPLICATIONS_DECLINED_DXP'
    from [dbo].[INSTACASH_APPLICATIONS]
    where STATUS = 'DECLINED'
    and bot_used = 'DXP'

    
`;

context.log(`---query3b---`);
context.log(queryText);
context.log(`---query3b---`);

sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
if (sqlQueryResult != 'fail') 
{
sqlQueryList.push(sqlQueryResult);
context.log(`sqlQuery3b SUCCESS`);
} else
{
context.log(`sqlQuery3b FAILED`);
}

context.log(`---query3bresult---`);
context.log(sqlQueryResult);
context.log(`---query3bresult---`);

//QUICKBILLS

queryText = 
`SELECT COUNT(*) AS 'QUICKBILLS_ENROLLMENT_PAST24HRS'
FROM [dbo].[QuickBills_Enrollment]
 WHERE enrollmentdate >= DATEADD(HOUR, -24, CAST('${dateToday}' AS datetime2));


SELECT COUNT(*) AS QUICKBILLS_OK_FOR_BILLING_COUNT
FROM [dbo].[QuickBills]
WHERE statusDescription = 'OK FOR BILLING';

SELECT COUNT(*) AS TOTAL_QUICKBILLS_RECORDS
FROM [dbo].[QuickBills];
`;

context.log(`---QUICKBILLS---`);
context.log(queryText);
context.log(`---QUICKBILLS---`);

sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prod1-SEA');
if (sqlQueryResult != 'fail') 
{
sqlQueryList.push(sqlQueryResult);
context.log(`quickbills SUCCESS`);
} else
{
context.log(`quickbills FAILED`);
}

context.log(`---quickbills---`);
context.log(sqlQueryResult);
context.log(`---quickbills---`);


 context.log(`sqlQueryList ==`);  
 context.log(sqlQueryList);  

queryText = 
`  
    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BAL_TRAN_AMOUNT_THIS_MONTH'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}' 

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BAL_TRAN_AMOUNT_LAST_MONTH'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
    CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}' 

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BALANCE_TRANSFER_AMOUNT_APPROVED_TOTAL_PHP'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED'

    SELECT COUNT(*) *250 as 'BALANCE_TRANSFER_TOTAL_PROCESSING_FEE_COLLECTED_PHP'
    FROM [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and [PROCESSING_FEE_CONSENT] = 'YES'

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BALANCE_TRANSFER_AMOUNT_TOTAL_PHP'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_TOTAL'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_APPROVED'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED'

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_DECLINED'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'DECLINED' 


    -----------------------------

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BAL_TRAN_AMOUNT_THIS_MONTH_ESTA'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BAL_TRAN_AMOUNT_LAST_MONTH_ESTA'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
    CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BALANCE_TRANSFER_AMOUNT_APPROVED_TOTAL_PHP_ESTA'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    SELECT COUNT(*) *250 as 'BALANCE_TRANSFER_TOTAL_PROCESSING_FEE_COLLECTED_PHP_ESTA'
    FROM [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and [PROCESSING_FEE_CONSENT] = 'YES'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BALANCE_TRANSFER_AMOUNT_TOTAL_PHP_ESTA'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    WHERE (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_TOTAL_ESTA'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    WHERE (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_APPROVED_ESTA'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_DECLINED_ESTA'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'DECLINED'
    and (bot_used = 'ESTA-SDK4-WEB' OR bot_used IS NULL OR LTRIM(RTRIM(bot_used)) = '')

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BAL_TRAN_AMOUNT_THIS_MONTH_DXP'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.thisMonthDatetime2}'
    and BOT_USED = 'DXP'

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BAL_TRAN_AMOUNT_LAST_MONTH_DXP'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and
    CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}' and
    CAST(CREATED AS datetime2) < '${userData.requestBotStats.thisMonthDatetime2}'
    and BOT_USED = 'DXP'

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BALANCE_TRANSFER_AMOUNT_APPROVED_TOTAL_PHP_DXP'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED'
    and BOT_USED = 'DXP'

    SELECT COUNT(*) *250 as 'BALANCE_TRANSFER_TOTAL_PROCESSING_FEE_COLLECTED_PHP_DXP'
    FROM [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED' and [PROCESSING_FEE_CONSENT] = 'YES'
    and BOT_USED = 'DXP'

    SELECT SUM(CAST(replace(AMOUNT, ',', '') as float)) as 'BALANCE_TRANSFER_AMOUNT_TOTAL_PHP_DXP'
    FROM  [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    WHERE BOT_USED = 'DXP'

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_TOTAL_DXP'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    WHERE BOT_USED = 'DXP'

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_APPROVED_DXP'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'APPROVED'
    and BOT_USED = 'DXP'

    select count(*)   as 'BALANCE_TRANSFER_APPLICATIONS_DECLINED_DXP'
    from [dbo].[BALANCE_TRANSFER_APPLICATIONS]
    where STATUS = 'DECLINED'
    and BOT_USED = 'DXP'

    SELECT count (*)    as 'CARDS_ACTIVATED_SINCE_2021_10_20'
    FROM [dbo].[CARD_ACTIVATION]

    SELECT count (*) as 'CARDS_ACTIVATED_DAILY' FROM
    [dbo].[CARD_ACTIVATION] WHERE CAST(CREATED AS datetime2) = CAST(DATEADD(day, -1, '${dateToday}') AS datetime2)
    AND (STATUS LIKE '%ACTIVA%' OR STATUS = 'PENDING (UNDELIVERED)')

    SELECT count (*) as 'CARDS_ACTIVATED_LAST_MONTH' FROM [dbo].[CARD_ACTIVATION] 
    WHERE CAST(CREATED AS datetime2) BETWEEN '${userData.requestBotStats.lastMonthDatetime2}' and '${userData.requestBotStats.thisMonthDatetime2}'
    AND (STATUS LIKE '%ACTIVA%' OR STATUS = 'PENDING (UNDELIVERED)')

    
`;

context.log(`---query3c---`);
context.log(queryText);
context.log(`---query3c---`);

sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
if (sqlQueryResult != 'fail') 
{
sqlQueryList.push(sqlQueryResult);
context.log(`sqlQuery3c SUCCESS`);
} else
{
context.log(`sqlQuery3c FAILED`);
}

context.log(`---query3cresult---`);
context.log(sqlQueryResult);
context.log(`---query3cbresult---`);


 context.log(`sqlQueryList ==`);  
 context.log(sqlQueryList);  
//*END
    //CREATE CSV STRING
    //merge sqlQuery
    // context.log(`create csv start ===`);
    // let exportObj = Object.assign({}, sqlQuery, sqlQuery2);
    // csvText = csvmaker1(exportObj);
    // console.log(`exportObj ===`, exportObj);
    // console.log(`csvText ===`, csvText);
    //  context.log(`create csv end ===`);

//     //SQL INSERT COLUMN NAMES
//     let reportInsertSQLcolumnNames = DisplayBotStatsResult(sqlQuery).sqlColumnNames;
//     reportInsertSQLcolumnNames = `${reportInsertSQLcolumnNames}, ${DisplayBotStatsResult(sqlQuery2).sqlColumnNames}` 
//     reportInsertSQLcolumnNames = `${reportInsertSQLcolumnNames}, ${DisplayBotStatsResult(sqlQuery3).sqlColumnNames}` 

//    //SQL INSERT VALUES
//     let reportInsertSQLvalues = DisplayBotStatsResult(sqlQuery).sqlColumnValues;
//     reportInsertSQLvalues = `${reportInsertSQLvalues}, ${DisplayBotStatsResult(sqlQuery2).sqlColumnValues}` 
//     reportInsertSQLvalues = `${reportInsertSQLvalues}, ${DisplayBotStatsResult(sqlQuery3).sqlColumnValues}` 

//     //CREATE A MERGED OBJECT FROM THE SQL RESULTS
//     let resultObject = {
//     ...DisplayBotStatsResult(sqlQuery).resultObject,
//     ...DisplayBotStatsResult(sqlQuery2).resultObject,
//     ...DisplayBotStatsResult(sqlQuery3).resultObject
//     };

    let reportInsertSQLcolumnNames = ``;
    let reportInsertSQLvalues = ``;
    let resultObject = {};
    let resultReport = ``; 
    for (let item in sqlQueryList)
    {
        if (item == 0)
        {
            reportInsertSQLcolumnNames = DisplayBotStatsResult(sqlQueryList[item]).sqlColumnNames;
            reportInsertSQLvalues = DisplayBotStatsResult(sqlQueryList[item]).sqlColumnValues;
            resultReport = `${DisplayBotStatsResult(sqlQueryList[item]).currentMessageGroup}`;
        }
        else
        {
            reportInsertSQLcolumnNames = `${reportInsertSQLcolumnNames}, ${DisplayBotStatsResult(sqlQueryList[item]).sqlColumnNames}` 
            reportInsertSQLvalues = `${reportInsertSQLvalues}, ${DisplayBotStatsResult(sqlQueryList[item]).sqlColumnValues}` 
            resultReport = `${resultReport} <br> ${DisplayBotStatsResult(sqlQueryList[item]).currentMessageGroup}`;
        }

        let tempObject = DisplayBotStatsResult(sqlQueryList[item]).resultObject
        resultObject = 
        {   ...resultObject,
            ...DisplayBotStatsResult(sqlQueryList[item]).resultObject,
        }

    } 

    context.log(`reportInsertSQLcolumnNames ==`);    
    context.log(reportInsertSQLcolumnNames);
    context.log(`reportInsertSQLvalues ==`);    
    context.log(reportInsertSQLvalues);

  
    context.log(`resultObject =========================`);    
    context.log(resultObject);
    context.log(`resultObject end =========================`);    

 
 
    //USE MERGED OBJECT TO
    //CREATE CSV
    let csvOutput = csvmaker1(resultObject, context);
    context.log(`csvOutput ==`);    
    context.log(csvOutput);
    context.log(`csvOutput end`);


    //LOG REPORT IN SQL
    let reportInsertSQLQuery = 
    `INSERT INTO REQUEST_BOT_STATS_HISTORY
    (
        CREATION_DATE,
        ${reportInsertSQLcolumnNames}
    )
    VALUES
    (
        '${GetSQLDateTime()}',
        ${reportInsertSQLvalues}

    )
    `;

       context.log(`reportInsertSQLQuery ==============`);
        context.log(reportInsertSQLQuery);
       context.log(`reportInsertSQLQueryEND ==============`);

    let reportInsertSQL = await sqlQueryCustomerDB(context, reportInsertSQLQuery, 'dev1-SEA');
    if (reportInsertSQL != 'fail') 
    {
        context.log(`reportInsertSQL SUCCESS`);
    } else
    {
        context.log(`reportInsertSQL FAILED`);
        context.log(`some columns might not exist in the table`);
    }


    //EMAIL REPORT
    // resultReport = `${DisplayBotStatsResult(sqlQuery).currentMessageGroup}`;
    // resultReport = `${resultReport} <br> ${DisplayBotStatsResult(sqlQuery2).currentMessageGroup}`;
    // resultReport = `${resultReport} <br> ${DisplayBotStatsResult(sqlQuery3).currentMessageGroup}`;
   

    let emailSubject =`DAILY REPORT REQUEST BOT STATS ${dateToday}`
    let emailBody =  `TIME GENERATED ${timeStamp} <br> <br>${new Date()} <br><br> ${resultReport}`;
        emailBody =  `${emailBody} <br> <br> `;
    let emailAddresses =`RADuazo@eastwestbanker.com`
    //let emailAddresses =`scijares@eastwestbanker.com`
    //let emailAddresses =`gltordecilla@eastwestbanker.com; ajmanalo@eastwestbanker.com; scijares@eastwestbanker.com`
   // let emailAddresses = `aagumiran@eastwestbanker.com; eodelacruz@eastwestbanker.com;plrodriguez@eastwestbanker.com; gltordecilla@eastwestbanker.com; ajmanalo@eastwestbanker.com; scijares@eastwestbanker.com`
    //  let emailAddresses = `lmmanantan@eastwestbanker.com`
    //
    //
    //context.log(`emailBody = `, emailBody);

    //await sendEmail(context, 'PROD', emailAddresses, emailSubject, emailBody, '', '')
   
    //CREATE LOCAL FILE
    let fileName = `DAILY_CHATBOT_REPORT-${DateToday()}-${getTimeStamp()}.csv`
    let filePath = `${fileName}`
    let webhookUrl = '';
    createFile(context, filePath, csvOutput);

    //SEND EMAIL
    axiosUploadFileAndEmail(context, filePath, fileName, 'text/csv' , emailAddresses, emailBody, emailSubject);

    //DELETE FILE
    fs.unlink(filePath,
    function (err)
    {
        if (err) throw err;
        // if no error, file has been deleted successfully
        //console.log(`${originalImage} file deleted!`);
    });

 context.log('JavaScript timer trigger function ran!', timeStamp);  
    context.res = {
        // status: 200, /* Defaults to 200 */
        body: emailBody
    };

    return emailBody; 
};

async function sqlQueryCustomerDB(context, queryText, webhookDestination = 'prodCentralUS')
{

    var userData = {};
    let sqlResult = null;

    if (typeof queryText == 'undefined')
    {
        context.log(`sqlQueryCustomerDB failed : queryText is undefined`)
        return 'failed';
    }

    //LOGIC APP esta-sdk4-logic-sqlQuery
    var webhookUrl = 'https://prod-11.southeastasia.logic.azure.com:443/workflows/7c7dd28dcd374a3082023af5dc453825/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=z5eMlbNA1_YfMXCy63AeJoYHQiRxBKPOkvIxlQgX_RY';

    switch (webhookDestination)
    {
        case 'prod1-SEA':
            //LOGIC APP esta-sdk4-logic-sqlQuery-dcbsdMaster
            webhookUrl = 'https://prod-21.southeastasia.logic.azure.com:443/workflows/8049d92c64fc4015b35884ec23b44335/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=zg6W-DPGze9TwbTmStfjeWOBbwydNmNmoB5f4ez01IQ';
            break;
        case 'dev1-SEA':
            //LOGIC APP esta-sdk4-logic-sqlQuery-dcbsdMaster-dev1
            webhookUrl = 'https://prod-07.southeastasia.logic.azure.com:443/workflows/6c41bac1ccaf41638bcb6bece6395aba/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=t21DYe09u2c3vphmtgXL3WIRpib4zfZ0C9fHskYub8c';
            break;
        case 'prodCentralUS':
            //LOGIC APP esta-sdk4-logic-sqlQuery
            webhookUrl = 'https://prod-11.southeastasia.logic.azure.com:443/workflows/7c7dd28dcd374a3082023af5dc453825/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=z5eMlbNA1_YfMXCy63AeJoYHQiRxBKPOkvIxlQgX_RY';
            break;
        case 'feedbackCentralUS':
            //LOGIC APP esta-sdk4-logic-sqlQuery-feedbackCentralUS
            webhookUrl = 'https://prod-11.southeastasia.logic.azure.com:443/workflows/7c7dd28dcd374a3082023af5dc453825/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=z5eMlbNA1_YfMXCy63AeJoYHQiRxBKPOkvIxlQgX_RY';
            break;
        case 'rts':
            //LOGIC APP esta-sdk4-logic-sqlQuery-RTS
            webhookUrl = 'https://prod-22.southeastasia.logic.azure.com:443/workflows/c6f5c4c118e14c26a548f4ef962ff923/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5S5J52mIC1J-nCQmW4T0xsmDfAOvpJNXgaH-EQpMEzY';
            break;
        case 'dev1':
            //LOGIC APP esta-sdk4-logic-sqlQueryDev
            webhookUrl = 'https://prod-09.southeastasia.logic.azure.com:443/workflows/4893d4a68c4c4f5691593b08190d6216/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=-B9OsPlQsf5yEixzOo85X1XBraDT28J6db_o4wC-i7o';
            break;
        case 'botLoginToken':
            //LOGIC APP esta-sdk4-logic-sqlQuery-botLoginToken
            webhookUrl = 'https://prod-31.southeastasia.logic.azure.com:443/workflows/28579b317c924e8eb55c72f3535ede84/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=N8OJGXVY4h8721fUIdXz8Xy4qq0X0EI7LVNUDW-cmwk';
            break;
        case 'ccts':
            //esta-sdk4-logic-sqlQuery-CCTS
            webhookUrl = 'https://prod-49.southeastasia.logic.azure.com:443/workflows/d08e6871848d4aada8c3db7da9c68283/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=eJHUoeLi267G8gQBzOWUDNTtk83FHbh8kdum0IOS0No';
            break;
        case 'chatbotDemo':
            //esta-sdk4-logic-sqlQuery-chatbot_demo
            webhookUrl = 'https://prod-24.southeastasia.logic.azure.com:443/workflows/bf99379e78cb45c7bf465631d4a966cf/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=mmlhfi-bSAeOCml4dbxrAKyB-auVTOLO9epFRRKjyO8';
            break;
        case 'refBot':
            //LOGIC APP esta-sdk4-logic-sqlQuery-refBot
            webhookUrl = 'https://prod-63.southeastasia.logic.azure.com:443/workflows/5e795893653041ac94d79c0745e5d291/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=zbTyfFUrWXB633giL29HyK15lytDPPBDiFj0VdV629w';
            break;
        case 'ConsumersLoan':
            //LOGIC APP consumers-loan
            webhookUrl = 'https://prod-51.southeastasia.logic.azure.com:443/workflows/51f10da95b0e46ea8016a15dd607a2f6/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=I7dCfwh3EdV-xnreIJc5Hg-g9hCUPa2FxitgrhpwD5s';
            break;

        case 'CardsAcqui':
            //LOGIC APP esta-sdk4-logic-sqlQuery-cardsAcqui
            webhookUrl = 'https://prod-30.southeastasia.logic.azure.com:443/workflows/40f23d346252455c98d6042b89a5a0e7/triggers/request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Frequest%2Frun&sv=1.0&sig=uuIsnVz0TdDfFRuYC9lorjBI8FufhlBQb4npKc3OAGk';
            break;
        default:
            throw new Error(`sqlQueryCustomerDB : webhookDestination NO MATCH`)
            break;
    }

    let body =
    {
        "Mobile Number": `${userData.mobile}`,
        "queryText": `${queryText}`
    }
    let config = {
        method: 'post',
        url: webhookUrl,
        data: body
    }
    let result;
    let promiseName = 'sqlQueryCustomerDB'
    let promise = axios(config)
        .then(function (response)
        {
            result = response.data;
            context.log(`---begin success sqlQueryCustomerDB---`);
            context.log(response.data);
            context.log(`---end success sqlQueryCustomerDB---`);
        })
        .catch(function (error)
        {
            result = 'sqlQueryCustomerDB error';
            context.log(`---begin fail sqlQueryCustomerDB---`);
            context.log(error);
            context.log(`---end fail sqlQueryCustomerDB---`);
        });

    //WAIT FOR PROMISE
    var requestResult = await waitForAxios(context, promise, promiseName);
    requestResult.body = result;

    if (result != 'sqlQueryCustomerDB error')
    {
        userData.sqlResult = requestResult.body;
        return userData.sqlResult
    }
    else
    {
        userData.sqlResult = 'fail';
        return 'fail'
    }

    //return requestResult;
}

    async function waitForAxios(context, promise, promiseName, maxTime)
    {
        var maxTimeToWait = 500;

        var requestResult = {};
        requestResult.status = 'pending';

        promise
            .then(function (response)
            {
                requestResult.status = 'success';
            })
            .catch(function (error)
            {
                requestResult.error = error;
                context.log(`${promiseName} ERROR: ` + error);
                requestResult.status = 'failed';
            });

        var timer = 0;
        for (var i = 0; i <= maxTimeToWait; i++)
        {
            timer++

            if (timer >= maxTimeToWait)
            {
                requestResult.status = "timeout";
                context.log(`${promiseName} TIMEOUT. QUERY TOOK TOO LONG (greater than ${maxTimeToWait} secs)`);
                break;
            }
            await new Promise(resolve => setTimeout(resolve, 1000));
            if (requestResult.status != 'pending')
            {
                break;
            }
        }

  
        {
            context.log(`I waited ${timer} second/s for the result.`);
        }

        return requestResult;
    }

    async function waitTimer(secsToWait)
    {

        var timer = 0;
        for (var i = 0; i < secsToWait; i++)
        {
            timer++
            await new Promise(resolve => setTimeout(resolve, 1000));
        }

        return;
    }

function GetFullDateTime()
{
    var offset = +8; //Philippine Time
    var currentDate = new Date(new Date().getTime() + offset * 3600 * 1000).toUTCString().replace(/ GMT$/, "");
    return currentDate;
}


function GetSQLDateTime()
{
    var offset = 8; //Philippine Time
    //var currentDate = new Date(new Date().getTime() + offset * 3600 * 1000).toUTCString().replace(/ GMT$/, "");
    var currentDate = new Date();

    var date = new Date();

    //GET TIMEZONE OFFSET AND CONVERT TO HOURS
    var timeZomeOffset = ((new Date().getTimezoneOffset()) / 60) * -1;

    //ADD CURRENT OFFSET SO WHEN toISOSTRING CONVERTS time to UTC IT WILL RETAIN CURRENT offsset
    if (timeZomeOffset == 0)
    {
        date = date.setHours(date.getHours() + offset);
    }

    console.log(new Date(date).toISOString().slice(0, 19).replace('T', ' '));
    return new Date(date).toISOString().slice(0, 19).replace('T', ' ');
}

async function sendEmail(context, environment = 'PROD', emailAddress, subject, body, attachmentFilename, attachmentLink)
{
    let userData = {};

    let currEnvironment = 'PROD'

    //emailAddress seperate email addresses with ; semicolon
    //ex. aagumiran@eastwestbanker.com;anton.gumiran@gmail.com

    //PROD esta-sdk4-logic-sendEmail
    let webhookUrl = 'https://prod-12.southeastasia.logic.azure.com:443/workflows/542cbea82b6d483396ef6c7c90f95830/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=LOKM8EmeFXhNrJgi8x1DYmKg3Zsyb8Wgbep-81YFKNQ';

    //NO DEV ENVIRONTMENT AVAILABLE

    var miliDate = new Date(GetFullDateTime());

    let bodyOptions = {
        "emailAddress": `${emailAddress}`,
        "subject": `${subject}`,
        "body": `${body}`
    }

    if (CheckIfObjectIsDefined(attachmentFilename) == true && CheckIfObjectIsDefined(attachmentLink) == true)
    {
        bodyOptions.attachmentFilename = `${attachmentFilename}`;
        bodyOptions.attachmentLink = `${attachmentLink}`;
    }

    let options;
    if (currEnvironment == 'DEV')
    {
        bodyOptions.subject = `TEST ONLY : ${subject}`;

        options =
        {
            url: webhookUrl,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(bodyOptions)
        };
    }
    else
    {
        options =
        {
            url: webhookUrl,
            headers: {
                'Content-Type': 'application/json'
            },
            body: JSON.stringify(bodyOptions)
        };
    }

    var promiseName = 'SEND EMAIL';
    //var promise = axios.post(webhookUrl, bodyOptions);

    let config = {
        method: 'post',
        url: webhookUrl,
        data: bodyOptions
    }
    let promise = axios(config)
        .then(function (response)
        {
            context.log(`---${promiseName}---`);
            context.log(bodyOptions);
            context.log(`---${promiseName}---`);
        })
        .catch(function (error)
        {
            context.log(`---${promiseName}---`);
            context.log(error);
            context.log(`---${promiseName}---`);
        });

    //WAIT FOR PROMISE
    var requestResult = await waitForAxios(context, promise, promiseName);

    return requestResult;
}

 function axiosUploadFileAndEmail(context, filePath, fileName, contentType, emailAddress, emailBody, emailSubject)
{
    const path = filePath;//'/path/to/your/file.{extension}'
    //const file = fs.readFileSync(path) /** Read file */

    let file = fs.readFileSync(path, {encoding: 'utf8'});

    let base64file = file.toString('base64');
    file = base64file;

    const stats = fs.statSync(path) /** Get file size in bytes (for content-length) */
    const fileSizeInBytes = stats.size;

    let contentType2 = 'application/octet-stream';
    if (typeof contentType != 'undefined')
    {
        contentType2 = contentType;
    }
    /** Add appropriate headers */
    const headers = {
        //'Authorization': 'Bearer Your Token', /** Optional */
        'Content-Length': fileSizeInBytes, /** Recommended to add it */
        'Content-Type': 'application/json',
    }
    //let url = webhookUrl;//"https://www.example.com/remote-server-upload-url"

    //esta-sdk4-uploadFile
    let webhookUrl = 'https://prod-45.southeastasia.logic.azure.com:443/workflows/20993d042d584e319c2170ab7544fc4f/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=CFdqv0xVY3Ua7YCLhPXT6TiyyQQZfsCHCMkr43_1KME';
    
    console.log(`axiosUploadFile1`)
    console.log(`path ==`, path);
    console.log(`stats ==`, stats);
    console.log(`fileSizeInBytes ==`, fileSizeInBytes);

    //GET FILE EXTENSION
    let uploadFileType = fileName.replace(/^.*[\\\/]/, '');
    uploadFileType = uploadFileType.split('/').pop().split('?')[0];
    uploadFileType = uploadFileType.split('.')[1];

    let axiosConfig = 
    {
        headers: headers,
        maxContentLength: Infinity, /** To avoid max content length error */
        maxBodyLength: Infinity /** To avoid max body length error */
    }

    let body = 
    {
        "file" : file,
        "fileName" : fileName,
        "contentType" : contentType,
        "fileExtension" : uploadFileType,
        "emailAddress" : emailAddress,
        "emailSubject" : emailSubject,
        "emailBody" : emailBody,
    }

    // let promise = axios.post(url, body, axiosConfig)
    //     .then(function (response)
    //     {

    //     })
    //     .catch(function (error)
    //     {
    //         console.log(`axiosUploadFile3`, error)
    //     });


    //Wait for query result
    //let requestResult = await this.waitForRequest(step, promise, "axiosUploadFile");


    let config = {
        method: 'post',
        url: webhookUrl,
        data: body
    }
    let promise = axios(config)
        .then(function (response)
        {
            context.log(`---axiosUploadFileAndEmail---`);
            context.log(bodyOptions);
            context.log(`---axiosUploadFileAndEmail---`);
        })
        .catch(function (error)
        {
            context.log(`---axiosUploadFileAndEmail---`);
            context.log(error);
            context.log(`---axiosUploadFileAndEmail---`);
        });

    //WAIT FOR PROMISE
    var requestResult = waitForAxios(context, promise, `axiosUploadFileAndEmail`);

    return requestResult;
}


function CheckIfObjectIsDefined(object)
{
    if(typeof object == 'undefined' ||
       object == null ||
       object == 'null')
    {
        return false;
    }
    else
    {
        return true;
    }
}

function ConvertNullToZero(number)
{
    var convertedNumber = parseFloat(number);
    if (number != null && number != 'null')
        return convertedNumber;
    else
        return 0;
}

function DisplayBotStatsResult(result)
{
    var result = JSON.stringify(result);
    var parseResult = JSON.parse(result);
    var resultLength = Object.keys(parseResult).length;
    var messageArray = [];
    var currentMessageGroup = '';
    let loopResult = {};

    let sqlQuery = ``
    let sqlColumnNames = ``;
    let sqlColumnValues = ``;
    let resultObject = {};

    for (item = 0; item < resultLength; item++) 
    {
        var tableName = Object.keys(parseResult)[item]
        var getTableX = parseResult[`${tableName}`];
        var aliasName = Object.keys(getTableX[0])[0]
        var aliasValue = getTableX.map

        var values = Object.keys(getTableX).map(function (e)
        {
            return getTableX[e]
        });

        values = values[0][`${aliasName}`];
        values = ConvertNullToZero(values);

        var stringdisplay = `${values.toLocaleString()}`;
        if (countDecimalPlaces(values) > 0)
        {
            stringdisplay = `${values.toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
        }

        sqlColumnNames = `${sqlColumnNames}${aliasName} `;
        sqlColumnValues = `${sqlColumnValues}'${values}' `
        if(item < resultLength - 1)
        {
            sqlColumnNames = `${sqlColumnNames}, `;
            sqlColumnValues = `${sqlColumnValues}, `
        }

        resultObject[`${aliasName}`] = `${values}`
        
        let curMessage = `${aliasName.replace(/_/gi, ' ')} = ${stringdisplay}<br>`;
        messageArray.push(curMessage);

        currentMessageGroup = `${currentMessageGroup}${curMessage}`;

        // if (messageArray.length >= 10)
        // {
        //     return currentMessageGroup;
        //     currentMessageGroup = '';
        //     messageArray = [];
        // }

        // if (messageArray.length < 10 && resultLength == item + 1) 
        // {
        //     return currentMessageGroup;
        //     currentMessageGroup = '';
        //     messageArray = [];
        // }
    }

    loopResult.currentMessageGroup = currentMessageGroup;
    loopResult.sqlQuery = sqlQuery;
    loopResult.sqlColumnValues = sqlColumnValues;
    loopResult.sqlColumnNames = sqlColumnNames;
    loopResult.resultObject = resultObject;


    return loopResult;
}

function countDecimalPlaces(a)
{
    if (!isFinite(a)) return 0;
    var e = 1, p = 0;
    while (Math.round(a * e) / e !== a) { e *= 10; p++; }
    return p;
}

function DateToday()
{
    var offset = +8; //Philippine Time
    var currentDate = new Date(new Date().getTime() + offset * 3600 * 1000).toISOString().substr(0, 10).replace('T', ' ');
    return currentDate;
}

function formatDate(date)
{
    if (date.length <= 8)
    {
        date = date.slice(0, 4) + "-" + date.slice(4, 6) + "-" + date.slice(6, 8);
    }

    var d = new Date(date),
        month = '' + (d.getMonth() + 1),
        day = '' + d.getDate(),
        year = d.getFullYear();

    if (month.length < 2)
        month = '0' + month;
    if (day.length < 2)
        day = '0' + day;

    return [year, month, day].join('-');
}

function addDays(date, days)
{
    var result = new Date(date);
    result.setDate(result.getDate() + days);
    return result;
}


function createFile(filePath, content)
{
    fs.writeFileSync(filePath , content);
    console.log(`createFile success ==`, filePath);
}

function csvmaker1(data, context) 
{

    // Empty for storing the values
    let csvText = ''    
    
    // Get headers of object
    // This function expects a uniform set of headers for the content of all objects
    // Let's use the first item's header
    let headers = Object.keys(data);
    // context.log(`csvmaker1 headers ============================ `)
    // context.log(headers)
    csvText = `${headers}\n`;

    //HEADERS END


    //CREATE 1st row VALUES OF THE CSV
    let csvValues = '';
    for (let item in headers)
    {
        csvValues += `${data[headers[item]]}, `
    }

    csvText += `${csvValues}`
    //VALUES END
   
    return csvText
}

function createFile(context, filePath, content)
{
    fs.writeFileSync(filePath , content);
    context.log(`createFile success ==`, filePath);
}

function DateToday()
{
    //var dateToday = moment().format('YYYY-MM-DD');
    //return dateToday;
    var offset = +8; //Philippine Time

    //var currentDate = new Date(new Date().getTime() + offset * 3600 * 1000).toUTCString().replace(/ GMT$/, "");
    //let current_datetime = new Date(currentDate);
    //let formatted_date = current_datetime.getFullYear() + "-" + (current_datetime.getMonth() + 1) + "-" + current_datetime.getDate();
    //return formatted_date;

    var currentDate = new Date(new Date().getTime() + offset * 3600 * 1000).toISOString().substr(0, 10).replace('T', ' ');
    return currentDate;
}

function getTimeStamp()
{
    var date = new Date();
    return date.getTime();
    //return current time in milliseconds
}

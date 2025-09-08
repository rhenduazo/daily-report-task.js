const axios = require('axios').default;
const fs = require('fs');
const pathLib = require('path'); // for robust extension parsing

// >>> Recipients: used for both daily report and failure alerts (semicolon-separated)
const reportEmails = [
  'RADuazo@eastwestbanker.com',
  'scijares@eastwestbanker.com',
  'gltordecilla@eastwestbanker.com',
  'ajmanalo@eastwestbanker.com',
  'aagumiran@eastwestbanker.com',
  'eodelacruz@eastwestbanker.com',
  'plrodriguez@eastwestbanker.com',
  'lmmanantan@eastwestbanker.com'
].join('; ');

// Alerts go to the same list (change if you want a different alert list)
const alertEmails = reportEmails;

// --------------------
// Timezone-safe helpers (always compute PH time from UTC)
// --------------------
function nowPH() {
  // Always compute PH time from UTC to avoid double-adjustments
  const nowUtcMs = Date.now();
  return new Date(nowUtcMs + 8 * 60 * 60 * 1000);
}

function DateToday() { // YYYY-MM-DD in PH
  const d = nowPH();
  const y = d.getUTCFullYear();
  const m = String(d.getUTCMonth() + 1).padStart(2, '0');
  const day = String(d.getUTCDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function GetSQLDateTime() { // 'YYYY-MM-DD HH:mm:ss' in PH
  const d = nowPH();
  const iso = new Date(Date.UTC(
    d.getUTCFullYear(), d.getUTCMonth(), d.getUTCDate(),
    d.getUTCHours(), d.getUTCMinutes(), d.getUTCSeconds()
  )).toISOString();
  return iso.slice(0, 19).replace('T', ' ');
}

function GetFullDateTime() { // readable PH string
  return GetSQLDateTime(); // keep format consistent
}

// Safe formatDate
function formatDate(date) {
  // allow Date objects
  if (date instanceof Date) {
    let d = date;
    let month = '' + (d.getMonth() + 1);
    let day = '' + d.getDate();
    let year = d.getFullYear();
    if (month.length < 2) month = '0' + month;
    if (day.length < 2) day = '0' + day;
    return [year, month, day].join('-');
  }

  // strings (YYYYMMDD or YYYY-MM-DD)
  if (typeof date === 'string' && date.length === 8 && !date.includes('-')) {
    date = date.slice(0, 4) + "-" + date.slice(4, 6) + "-" + date.slice(6, 8);
  }

  var d = new Date(date),
      month = '' + (d.getMonth() + 1),
      day = '' + d.getDate(),
      year = d.getFullYear();

  if (month.length < 2) month = '0' + month;
  if (day.length < 2) day = '0' + day;

  return [year, month, day].join('-');
}

function addDays(dateStrYYYYMMDD, days) {
  // Accepts string 'YYYY-MM-DD' and returns Date
  var result = new Date(dateStrYYYYMMDD);
  result.setDate(result.getDate() + days);
  return result;
}

function nextDay(dateStrYYYYMMDD) {
  return formatDate(addDays(dateStrYYYYMMDD, 1));
}

module.exports = async function (context, myTimer) {
  const timeStamp = new Date(GetFullDateTime()).toISOString();
  context.log(timeStamp);

  if (myTimer.isPastDue) {
    // Alert: timer trigger ran late
    await sendDailyReportFailure(context, {
      emailAddresses: alertEmails,
      dateToday: DateToday(),
      stage: 'TIMER',
      reason: 'Timer trigger was past due (cold start, throttling, or host delay).',
      suggestion: 'Check Function App health/scale and CRON vs timezone.',
      extra: { timerInfo: 'isPastDue=true' }
    });
  }

  let csvText = '';
  let userData = {};
  const dateToday = DateToday();

  // Month anchors in PH time (padded)
  const now = nowPH();
  const thisMonthDatetime2 = `${now.getUTCFullYear()}-${String(now.getUTCMonth() + 1).padStart(2, '0')}-01 00:00:00.0000000`;
  const prev = new Date(Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), 1));
  prev.setUTCMonth(prev.getUTCMonth() - 1);
  const lastMonthDatetime2 = `${prev.getUTCFullYear()}-${String(prev.getUTCMonth() + 1).padStart(2, '0')}-01 00:00:00.0000000`;

  userData.requestBotStats = {
    thisMonthDatetime2,
    lastMonthDatetime2
  };

  context.log('GetFullDateTime() = ', GetFullDateTime());

  let sqlQueryList = [];
  let queryText;
  let sqlQueryResult;

  // ============ DEV1 QUERIES ============
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
  `;

  context.log(`---query1a---`);
  context.log(queryText);
  context.log(`---query1a---`);

  sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'dev1-SEA');
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`query1a SUCCESS`);
  } else {
    context.log(`query1a FAILED`);
    await notifySqlFail(context, 'query1a', 'dev1-SEA', queryText);
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
  `;

  context.log(`---query1b---`);
  context.log(queryText);
  context.log(`---query1b---`);
  sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'dev1-SEA');
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`query1b SUCCESS`);
  } else {
    context.log(`query1b FAILED`);
    await notifySqlFail(context, 'query1b', 'dev1-SEA', queryText);
  }

  context.log(`---query1result---`);
  context.log(sqlQueryResult);
  context.log(`---query1result---`);

  // ============ PROD US QUERIES 2A ============
  queryText = `
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
  `;

  context.log(`---query2a---`);
  context.log(queryText);
  context.log(`---query2a---`);

  sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery2a SUCCESS`);
  } else {
    context.log(`sqlQuery2a FAILED`);
    await notifySqlFail(context, 'sqlQuery2a', 'prodCentralUS', queryText);
  }

  context.log(`---query2aresult---`);
  context.log(sqlQueryResult);
  context.log(`---query2aresult---`);

  // ----- 2B
  queryText = `
    select count(*)   as 'BNPL_LOANS_APPLICATIONS_TOTAL'
    from [dbo].[CONVERT_TO_INSTALLMENT]
    where ( CTI_INTEREST_PROFILE = '3' or CTI_INTEREST_PROFILE = '4' ) and  CONVERT(VARCHAR, INSTALLMENT_MATRIX) = '3 months'

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
  `;

  context.log(`---query2b---`);
  context.log(queryText);
  context.log(`---query2b---`);

  sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery2b SUCCESS`);
  } else {
    context.log(`sqlQuery2b FAILED`);
    await notifySqlFail(context, 'sqlQuery2b', 'prodCentralUS', queryText);
  }

  context.log(`---query2bresult---`);
  context.log(sqlQueryResult);
  context.log(`---query2bresult---`);

  // -- 2C
  queryText = `
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
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery2c SUCCESS`);
  } else {
    context.log(`sqlQuery2c FAILED`);
    await notifySqlFail(context, 'sqlQuery2c', 'prodCentralUS', queryText);
  }

  context.log(`---query2aresult---`);
  context.log(sqlQueryResult);
  context.log(`---query2aresult---`);

  // *START additional PROD US QUERIES (3A)
  // Use end-exclusive windows to include the full end day
  const d0 = dateToday;
  const d1 = formatDate(addDays(d0, -1));
  const d7 = formatDate(addDays(d0, -7));
  const d30 = formatDate(addDays(d0, -30));
  const d60 = formatDate(addDays(d0, -60));
  const d90 = formatDate(addDays(d0, -90));
  const d180 = formatDate(addDays(d0, -180));
  const d0Next = nextDay(d0);

  queryText = `
    SELECT SUM(PRESS_START_COUNT) as PRESS_START_COUNT
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]

    SELECT SUM(STATEMENT_CHECK_COUNT)  as STATEMENT_CHECK_COUNT
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_24_HOURS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d1}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_7_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d7}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_30_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d30}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_60_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d60}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_90_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d90}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'

    SELECT count(*) as UNIQUE_INTERACTIONS_LAST_180_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d180}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_24_HOURS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d1}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'
      and REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_7_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d7}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'
      and REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_30_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d30}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'
      and REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_60_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d60}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'
      and REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_90_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d90}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'
      and REGISTERED = 'true'

    SELECT count(*) as UNIQUE_REGISTERED_INTERACTIONS_LAST_180_DAYS
    FROM [dbo].[BOT_INTERACTION_LOGS_NEW]
    WHERE TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) >= '${d180}'
      AND TRY_CONVERT(datetime, [DATE_LAST_INTERACTION], 120) <  '${d0Next}'
      and REGISTERED = 'true'

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
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery3a SUCCESS`);
  } else {
    context.log(`sqlQuery3a FAILED`);
    await notifySqlFail(context, 'sqlQuery3a', 'prodCentralUS', queryText);
  }

  context.log(`---query3aresult---`);
  context.log(sqlQueryResult);
  context.log(`---query3aresult---`);

  // 3B
  queryText = `
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
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery3b SUCCESS`);
  } else {
    context.log(`sqlQuery3b FAILED`);
    await notifySqlFail(context, 'sqlQuery3b', 'prodCentralUS', queryText);
  }

  context.log(`---query3bresult---`);
  context.log(sqlQueryResult);
  context.log(`---query3bresult---`);

  // QUICKBILLS
  queryText = `
    SELECT COUNT(*) AS 'QUICKBILLS_ENROLLMENT_PAST24HRS'
    FROM [dbo].[QuickBills_Enrollment]
    WHERE enrollmentdate >= DATEADD(HOUR, -24, CAST('${dateToday}' AS datetime2));

    SELECT COUNT(*) AS QUICKBILLS_OK_FOR_BILLING_COUNT
    FROM [dbo].[QuickBills]
    WHERE statusDescription = 'OK FOR BILLING';

    SELECT COUNT(*) AS TOTAL_QUICKBILLS_RECORDS
    FROM [dbo].[QuickBILLS];
  `;

  context.log(`---QUICKBILLS---`);
  context.log(queryText);
  context.log(`---QUICKBILLS---`);

  sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prod1-SEA');
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`quickbills SUCCESS`);
  } else {
    context.log(`quickbills FAILED`);
    await notifySqlFail(context, 'quickbills', 'prod1-SEA', queryText);
  }

  context.log(`---quickbills---`);
  context.log(sqlQueryResult);
  context.log(`---quickbills---`);

  context.log(`sqlQueryList ==`);
  context.log(sqlQueryList);

  // BALANCE TRANSFER + ACTIVATIONS (3C)
  queryText = `
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

    -- Activations: previous day window inclusive via range
    SELECT count (*)    as 'CARDS_ACTIVATED_SINCE_2021_10_20'
    FROM [dbo].[CARD_ACTIVATION]

    SELECT count (*) as 'CARDS_ACTIVATED_DAILY' 
    FROM [dbo].[CARD_ACTIVATION] 
    WHERE CAST(CREATED AS datetime2) >= CAST(DATEADD(day, -1, '${dateToday}') AS datetime2)
      AND CAST(CREATED AS datetime2) <  CAST('${dateToday}' AS datetime2)

    SELECT count (*) as 'CARDS_ACTIVATED_LAST_MONTH' 
    FROM [dbo].[CARD_ACTIVATION] 
    WHERE CAST(CREATED AS datetime2) >= '${userData.requestBotStats.lastMonthDatetime2}'
      AND CAST(CREATED AS datetime2) <  '${userData.requestBotStats.thisMonthDatetime2}'
  `;

  context.log(`---query3c---`);
  context.log(queryText);
  context.log(`---query3c---`);

  sqlQueryResult = await sqlQueryCustomerDB(context, queryText, 'prodCentralUS');
  if (sqlQueryResult != 'fail') {
    sqlQueryList.push(sqlQueryResult);
    context.log(`sqlQuery3c SUCCESS`);
  } else {
    context.log(`sqlQuery3c FAILED`);
    await notifySqlFail(context, 'sqlQuery3c', 'prodCentralUS', queryText);
  }

  context.log(`---query3cresult---`);
  context.log(sqlQueryResult);
  context.log(`---query3cbresult---`);

  context.log(`sqlQueryList ==`);
  context.log(sqlQueryList);

  // Alert if nothing was collected (would mean no email/empty data)
  if (!sqlQueryList || sqlQueryList.length === 0) {
    await sendDailyReportFailure(context, {
      emailAddresses: alertEmails,
      dateToday,
      stage: 'DATA MERGE',
      reason: 'No SQL result sets collected; CSV would be empty.',
      suggestion: 'Check earlier SQL steps/logs and rerun.',
      requestResult: { status: 'no-data' }
    });
  }

  // CREATE CSV STRING
  let reportInsertSQLcolumnNames = ``;
  let reportInsertSQLvalues = ``;
  let resultObject = {};
  let resultReport = ``;

  for (let item in sqlQueryList) {
    const piece = DisplayBotStatsResult(sqlQueryList[item]);
    if (item == 0) {
      reportInsertSQLcolumnNames = piece.sqlColumnNames;
      reportInsertSQLvalues = piece.sqlColumnValues;
      resultReport = `${piece.currentMessageGroup}`;
    } else {
      reportInsertSQLcolumnNames = `${reportInsertSQLcolumnNames}, ${piece.sqlColumnNames}`;
      reportInsertSQLvalues = `${reportInsertSQLvalues}, ${piece.sqlColumnValues}`;
      resultReport = `${resultReport} <br> ${piece.currentMessageGroup}`;
    }

    resultObject = {
      ...resultObject,
      ...piece.resultObject,
    };
  }

  context.log(`reportInsertSQLcolumnNames ==`);
  context.log(reportInsertSQLcolumnNames);
  context.log(`reportInsertSQLvalues ==`);
  context.log(reportInsertSQLvalues);

  context.log(`resultObject =========================`);
  context.log(resultObject);
  context.log(`resultObject end =========================`);

  // USE MERGED OBJECT TO CREATE CSV
  let csvOutput = csvmaker1(resultObject, context);
  context.log(`csvOutput ==`);
  context.log(csvOutput);
  context.log(`csvOutput end`);

  // Alert if CSV is empty
  if (!csvOutput || !csvOutput.trim()) {
    await sendDailyReportFailure(context, {
      emailAddresses: alertEmails,
      dateToday,
      stage: 'CSV GENERATION',
      reason: 'CSV output is empty.',
      suggestion: 'Inspect merged resultObject and header mapping.'
    });
  }

  // LOG REPORT IN SQL
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
  )`;

  context.log(`reportInsertSQLQuery ==============`);
  context.log(reportInsertSQLQuery);
  context.log(`reportInsertSQLQueryEND ==============`);

  let reportInsertSQL = await sqlQueryCustomerDB(context, reportInsertSQLQuery, 'dev1-SEA');
  if (reportInsertSQL != 'fail') {
    context.log(`reportInsertSQL SUCCESS`);
  } else {
    context.log(`reportInsertSQL FAILED`);
    context.log(`some columns might not exist in the table`);
    // Alert if writing history failed
    await sendDailyReportFailure(context, {
      emailAddresses: alertEmails,
      dateToday,
      stage: 'HISTORY LOGGING',
      reason: 'Insert into REQUEST_BOT_STATS_HISTORY failed.',
      suggestion: 'Check table schema/columns and SQL permissions.',
      requestResult: { status: 'failed' }
    });
  }

  // EMAIL REPORT
  let emailSubject =`DAILY REPORT REQUEST BOT STATS ${dateToday}`;
  let emailBody =  `TIME GENERATED ${timeStamp} <br> <br>${new Date()} <br><br> ${resultReport}`;
  emailBody =  `${emailBody} <br> <br> `;

  // >>> Your requested commented options (do not delete)
  //let emailAddress = `RADuazo@eastwestbanker.com`;
  //let emailAddresses =`scijares@eastwestbanker.com`
  //let emailAddresses =`gltordecilla@eastwestbanker.com; ajmanalo@eastwestbanker.com; scijares@eastwestbanker.com`
   let emailAddresses = `aagumiran@eastwestbanker.com, eodelacruz@eastwestbanker.com, plrodriguez@eastwestbanker.com, gltordecilla@eastwestbanker.com, ajmanalo@eastwestbanker.com, scijares@eastwestbanker.com`;
  //  let emailAddresses = `lmmanantan@eastwestbanker.com`
  
  // >>> Actual recipients used:
   emailAddresses = reportEmails;

  // CREATE LOCAL FILE
  let fileName = `DAILY_CHATBOT_REPORT-${DateToday()}-${getTimeStamp()}.csv`;
  let filePath = `${fileName}`;
  createFile(context, filePath, csvOutput);

  // SEND EMAIL (upload + email)
  const uploadResult = await axiosUploadFileAndEmail(context, filePath, fileName, 'text/csv' , emailAddresses, emailBody, emailSubject);

  // Alert if upload/email webhook didn’t complete
  if (!uploadResult || uploadResult.status !== 'success') {
    await sendDailyReportFailure(context, {
      emailAddresses: alertEmails,
      dateToday,
      stage: 'UPLOAD+EMAIL',
      reason: 'File upload/email webhook did not complete successfully.',
      suggestion: 'Confirm Logic App status and request size limits; try resending the CSV.',
      requestResult: uploadResult || { status: 'unknown' },
      extra: { fileName }
    });
  }

  // DELETE FILE (non-fatal)
  fs.unlink(filePath, (err) => {
    if (err) context.log('unlink failed:', err);
  });

  context.log('JavaScript timer trigger function ran!', timeStamp);
  context.res = {
    // status: 200, /* Defaults to 200 */
    body: emailBody
  };

  return emailBody; 
};

// -------------------- helpers below --------------------

async function sqlQueryCustomerDB(context, queryText, webhookDestination = 'prodCentralUS') {
  var userData = {};
  let sqlResult = null;

  if (typeof queryText == 'undefined') {
    context.log(`sqlQueryCustomerDB failed : queryText is undefined`);
    return 'failed';
  }

  // default
  var webhookUrl = 'https://prod-11.southeastasia.logic.azure.com:443/workflows/7c7dd28dcd374a3082023af5dc453825/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=z5eMlbNA1_YfMXCy63AeJoYHQiRxBKPOkvIxlQgX_RY';

  switch (webhookDestination) {
    case 'prod1-SEA':
      webhookUrl = 'https://prod-21.southeastasia.logic.azure.com:443/workflows/8049d92c64fc4015b35884ec23b44335/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=zg6W-DPGze9TwbTmStfjeWOBbwydNmNmoB5f4ez01IQ';
      break;
    case 'dev1-SEA':
      webhookUrl = 'https://prod-07.southeastasia.logic.azure.com:443/workflows/6c41bac1ccaf41638bcb6bece6395aba/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=t21DYe09u2c3vphmtgXL3WIRpib4zfZ0C9fHskYub8c';
      break;
    case 'prodCentralUS':
      webhookUrl = 'https://prod-11.southeastasia.logic.azure.com:443/workflows/7c7dd28dcd374a3082023af5dc453825/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=z5eMlbNA1_YfMXCy63AeJoYHQiRxBKPOkvIxlQgX_RY';
      break;
    case 'feedbackCentralUS':
      webhookUrl = 'https://prod-11.southeastasia.logic.azure.com:443/workflows/7c7dd28dcd374a3082023af5dc453825/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=z5eMlbNA1_YfMXCy63AeJoYHQiRxBKPOkvIxlQgX_RY';
      break;
    case 'rts':
      webhookUrl = 'https://prod-22.southeastasia.logic.azure.com:443/workflows/c6f5c4c118e14c26a548f4ef962ff923/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=5S5J52mIC1J-nCQmW4T0xsmDfAOvpJNXgaH-EQpMEzY';
      break;
    case 'dev1':
      webhookUrl = 'https://prod-09.southeastasia.logic.azure.com:443/workflows/4893d4a68c4c4f5691593b08190d6216/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=-B9OsPlQsf5yEixzOo85X1XBraDT28J6db_o4wC-i7o';
      break;
    case 'botLoginToken':
      webhookUrl = 'https://prod-31.southeastasia.logic.azure.com:443/workflows/28579b317c924e8eb55c72f3535ede84/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=N8OJGXVY4h8721fUIdXz8Xy4qq0X0EI7LVNUDW-cmwk';
      break;
    case 'ccts':
      webhookUrl = 'https://prod-49.southeastasia.logic.azure.com:443/workflows/d08e6871848d4aada8c3db7da9c68283/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=eJHUoeLi267G8gQBzOWUDNTtk83FHbh8kdum0IOS0No';
      break;
    case 'chatbotDemo':
      webhookUrl = 'https://prod-24.southeastasia.logic.azure.com:443/workflows/bf99379e78cb45c7bf465631d4a966cf/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=mmlhfi-bSAeOCml4dbxrAKyB-auVTOLO9epFRRKjyO8';
      break;
    case 'refBot':
      webhookUrl = 'https://prod-63.southeastasia.logic.azure.com:443/workflows/5e795893653041ac94d79c0745e5d291/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=zbTyfFUrWXB633giL29HyK15lytDPPBDiFj0VdV629w';
      break;
    case 'ConsumersLoan':
      webhookUrl = 'https://prod-51.southeastasia.logic.azure.com:443/workflows/51f10da95b0e46ea8016a15dd607a2f6/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=I7dCfwh3EdV-xnreIJc5Hg-g9hCUPa2FxitgrhpwD5s';
      break;
    case 'CardsAcqui':
      webhookUrl = 'https://prod-30.southeastasia.logic.azure.com:443/workflows/40f23d346252455c98d6042b89a5a0e7/triggers/request/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Frequest%2Frun&sv=1.0&sig=uuIsnVz0TdDfFRuYC9lorjBI8FufhlBQb4npKc3OAGk';
      break;
    default:
      throw new Error(`sqlQueryCustomerDB : webhookDestination NO MATCH`);
  }

  const body = {
    // Removed "Mobile Number" undefined field to avoid schema issues
    queryText: `${queryText}`
  };

  const config = { method: 'post', url: webhookUrl, data: body };

  let result;
  const promiseName = 'sqlQueryCustomerDB';
  const promise = axios(config)
    .then(function (response) {
      result = response.data;
      context.log(`---begin success sqlQueryCustomerDB---`);
      context.log(response.data);
      context.log(`---end success sqlQueryCustomerDB---`);
    })
    .catch(function (error) {
      result = 'sqlQueryCustomerDB error';
      context.log(`---begin fail sqlQueryCustomerDB---`);
      context.log(error);
      context.log(`---end fail sqlQueryCustomerDB---`);
    });

  // WAIT FOR PROMISE
  var requestResult = await waitForAxios(context, promise, promiseName);
  requestResult.body = result;

  if (result != 'sqlQueryCustomerDB error') {
    userData.sqlResult = requestResult.body;
    return userData.sqlResult;
  } else {
    userData.sqlResult = 'fail';
    return 'fail';
  }
}

// Shorter, safer wait (default 180s). Increase if your Logic App regularly runs longer.
async function waitForAxios(context, promise, promiseName, maxTimeSeconds = 180) {
  var maxTimeToWait = maxTimeSeconds;

  var requestResult = { status: 'pending' };

  promise
    .then(function () {
      requestResult.status = 'success';
    })
    .catch(function (error) {
      requestResult.error = error;
      context.log(`${promiseName} ERROR: ` + error);
      requestResult.status = 'failed';
    });

  let timer = 0;
  for (let i = 0; i <= maxTimeToWait; i++) {
    timer++;
    if (timer >= maxTimeToWait) {
      requestResult.status = "timeout";
      context.log(`${promiseName} TIMEOUT. QUERY TOOK TOO LONG (greater than ${maxTimeToWait} secs)`);
      break;
    }
    await new Promise(resolve => setTimeout(resolve, 1000));
    if (requestResult.status !== 'pending') break;
  }

  context.log(`I waited ${timer} second/s for the result.`);
  return requestResult;
}

async function sendEmail(context, environment = 'PROD', emailAddress, subject, body, attachmentFilename, attachmentLink) {
  // FIX: use passed environment
  let currEnvironment = environment;

  //PROD esta-sdk4-logic-sendEmail
  let webhookUrl = 'https://prod-12.southeastasia.logic.azure.com:443/workflows/542cbea82b6d483396ef6c7c90f95830/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=LOKM8EmeFXhNrJgi8x1DYmKg3Zsyb8Wgbep-81YFKNQ';

  let bodyOptions = {
    emailAddress: `${emailAddress}`,
    subject: `${subject}`,
    body: `${body}`
  };

  if (CheckIfObjectIsDefined(attachmentFilename) === true && CheckIfObjectIsDefined(attachmentLink) === true) {
    bodyOptions.attachmentFilename = `${attachmentFilename}`;
    bodyOptions.attachmentLink = `${attachmentLink}`;
  }

  if (currEnvironment === 'DEV') {
    bodyOptions.subject = `TEST ONLY : ${subject}`;
  }

  const promiseName = 'SEND EMAIL';
  const config = { method: 'post', url: webhookUrl, data: bodyOptions };

  const promise = axios(config)
    .then(function () {
      context.log(`---${promiseName}---`);
      context.log(bodyOptions);
      context.log(`---${promiseName}---`);
    })
    .catch(function (error) {
      context.log(`---${promiseName}---`);
      context.log(error);
      context.log(`---${promiseName}---`);
    });

  //WAIT FOR PROMISE
  var requestResult = await waitForAxios(context, promise, promiseName);
  return requestResult;
}

// Failure alert email with reason and context
async function sendDailyReportFailure(context, {
  emailAddresses,
  dateToday,
  stage,                  // e.g. 'SQL QUERY', 'CSV GENERATION', 'UPLOAD+EMAIL'
  reason,                 // human-readable cause
  suggestion,             // optional remediation
  requestResult = null,   // optional { status, error }
  extra = {}              // optional { webhookUrl, queryPreview, fileName, env, timerInfo }
}) {
  try {
    const env = (extra.env || 'PROD');
    const nowISO = new Date(GetFullDateTime()).toISOString();

    let body = `
      <div style="font-family:Segoe UI,Arial,Helvetica,sans-serif;font-size:14px;line-height:1.4">
        <h2 style="margin:0 0 8px">Daily Chatbot Report – Delivery Issue</h2>
        <p><b>Date</b>: ${dateToday} &nbsp;|&nbsp; <b>Detected</b>: ${nowISO}</p>
        <p><b>Stage</b>: ${stage}</p>
        <p><b>Cause</b>: ${reason}</p>
        ${suggestion ? `<p><b>Suggested action</b>: ${suggestion}</p>` : ''}

        ${requestResult ? `
          <hr style="border:none;border-top:1px solid #ddd;margin:14px 0">
          <h3 style="margin:0 0 8px">Diagnostic</h3>
          <ul style="margin:0 0 8px 18px">
            <li><b>Status</b>: ${requestResult.status ?? 'n/a'}</li>
            ${requestResult.error ? `<li><b>Error</b>: <code>${sanitizeForHtml(String(requestResult.error))}</code></li>` : ''}
          </ul>
        ` : ''}

        ${Object.keys(extra || {}).length ? `
          <h3 style="margin:14px 0 8px">Context</h3>
          <ul style="margin:0 0 8px 18px">
            ${extra.webhookUrl ? `<li><b>Webhook</b>: <code>${sanitizeForHtml(extra.webhookUrl)}</code></li>` : ''}
            ${extra.queryPreview ? `<li><b>Query (preview)</b>: <code>${sanitizeForHtml(extra.queryPreview.slice(0, 400))}${extra.queryPreview.length > 400 ? '…' : ''}</code></li>` : ''}
            ${extra.fileName ? `<li><b>File</b>: ${sanitizeForHtml(extra.fileName)}</li>` : ''}
            ${extra.timerInfo ? `<li><b>Timer</b>: ${sanitizeForHtml(extra.timerInfo)}</li>` : ''}
          </ul>
        ` : ''}

        <p style="color:#666">This is an automated notice from the Daily Report job.</p>
      </div>
    `;

    const subject = `ALERT: Daily Report not delivered (${stage}) – ${dateToday}`;
    await sendEmail(context, env, emailAddresses, subject, body, '', '');
    context.log(`[sendDailyReportFailure] Alert sent for stage=${stage}`);
  } catch (e) {
    context.log(`[sendDailyReportFailure] FAILED to send alert email: ${e}`);
  }

  function sanitizeForHtml(str) {
    return str.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
  }
}

// Helper to notify SQL failures with reason
async function notifySqlFail(context, tag, destination, queryText) {
  await sendDailyReportFailure(context, {
    emailAddresses: alertEmails,
    dateToday: DateToday(),
    stage: `SQL QUERY (${tag} → ${destination})`,
    reason: 'SQL webhook returned fail/timeout or error status.',
    suggestion: 'Verify Logic App availability, keys, and DB health. Re-run the job.',
    requestResult: { status: 'failed' },
    extra: { webhookUrl: destination, queryPreview: queryText || '' }
  });
}

// Upload CSV + email via Logic App
async function axiosUploadFileAndEmail(context, filePath, fileName, contentType, emailAddress, emailBody, emailSubject) {
  const path = filePath;

  // Read as Buffer -> base64
  let fileBuffer = fs.readFileSync(path);
  let base64file = fileBuffer.toString('base64');

  const stats = fs.statSync(path);
  const fileSizeInBytes = stats.size;

  let contentType2 = typeof contentType !== 'undefined' ? contentType : 'application/octet-stream';

  // esta-sdk4-uploadFile
  let webhookUrl = 'https://prod-45.southeastasia.logic.azure.com:443/workflows/20993d042d584e319c2170ab7544fc4f/triggers/manual/paths/invoke?api-version=2016-10-01&sp=%2Ftriggers%2Fmanual%2Frun&sv=1.0&sig=CFdqv0xVY3Ua7YCLhPXT6TiyyQQZfsCHCMkr43_1KME';
  
  console.log(`axiosUploadFile1`);
  console.log(`path ==`, path);
  console.log(`stats ==`, stats);
  console.log(`fileSizeInBytes ==`, fileSizeInBytes);

  // File extension
  let uploadFileType = pathLib.extname(fileName).replace('.', '') || 'csv';

  let body = {
    file: base64file,
    fileName: fileName,
    contentType: contentType2,
    fileExtension: uploadFileType,
    emailAddress: emailAddress,
    emailSubject: emailSubject,
    emailBody: emailBody,
  };

  const config = {
    method: 'post',
    url: webhookUrl,
    data: body,
    headers: { 'Content-Type': 'application/json' },
    maxContentLength: Infinity,
    maxBodyLength: Infinity
  };

  const promise = axios(config)
    .then(function () {
      context.log(`---axiosUploadFileAndEmail---`);
      context.log(body);
      context.log(`---axiosUploadFileAndEmail---`);
    })
    .catch(function (error) {
      context.log(`---axiosUploadFileAndEmail---`);
      context.log(error);
      context.log(`---axiosUploadFileAndEmail---`);
    });

  //WAIT FOR PROMISE
  var requestResult = await waitForAxios(context, promise, `axiosUploadFileAndEmail`);
  return requestResult;
}

function CheckIfObjectIsDefined(object) {
  return !(typeof object == 'undefined' || object == null || object == 'null');
}

function ConvertNullToZero(number) {
  var convertedNumber = parseFloat(number);
  if (number != null && number != 'null') return convertedNumber;
  else return 0;
}

function DisplayBotStatsResult(result) {
  var result = JSON.stringify(result);
  var parseResult = JSON.parse(result);
  var resultLength = Object.keys(parseResult).length;
  var messageArray = [];
  var currentMessageGroup = '';
  let loopResult = {};

  let sqlQuery = ``;
  let sqlColumnNames = ``;
  let sqlColumnValues = ``;
  let resultObject = {};

  for (let item = 0; item < resultLength; item++) {
    var tableName = Object.keys(parseResult)[item];
    var getTableX = parseResult[`${tableName}`];
    var aliasName = Object.keys(getTableX[0])[0];

    var values = Object.keys(getTableX).map(function (e) {
      return getTableX[e];
    });

    values = values[0][`${aliasName}`];
    values = ConvertNullToZero(values);

    var stringdisplay = `${values.toLocaleString()}`;
    if (countDecimalPlaces(values) > 0) {
      stringdisplay = `${values.toLocaleString('en-US', { minimumFractionDigits: 2 })}`;
    }

    sqlColumnNames = `${sqlColumnNames}${aliasName} `;
    sqlColumnValues = `${sqlColumnValues}'${values}' `;
    if (item < resultLength - 1) {
      sqlColumnNames = `${sqlColumnNames}, `;
      sqlColumnValues = `${sqlColumnValues}, `;
    }

    resultObject[`${aliasName}`] = `${values}`;
    
    let curMessage = `${aliasName.replace(/_/gi, ' ')} = ${stringdisplay}<br>`;
    messageArray.push(curMessage);
    currentMessageGroup = `${currentMessageGroup}${curMessage}`;
  }

  loopResult.currentMessageGroup = currentMessageGroup;
  loopResult.sqlQuery = sqlQuery;
  loopResult.sqlColumnValues = sqlColumnValues;
  loopResult.sqlColumnNames = sqlColumnNames;
  loopResult.resultObject = resultObject;

  return loopResult;
}

function countDecimalPlaces(a) {
  if (!isFinite(a)) return 0;
  var e = 1, p = 0;
  while (Math.round(a * e) / e !== a) { e *= 10; p++; }
  return p;
}

// Single createFile()
function createFile(context, filePath, content) {
  fs.writeFileSync(filePath , content);
  context.log(`createFile success ==`, filePath);
}

function csvmaker1(data, context) {
  // Empty for storing the values
  let csvText = '';

  // Headers
  let headers = Object.keys(data);
  csvText = `${headers.join(',')}\n`;

  // 1st row values
  let rowValues = headers.map(h => `${data[h] ?? ''}`).join(',');
  csvText += rowValues;
  
  return csvText;
}

function getTimeStamp() {
  var date = new Date();
  return date.getTime();
}

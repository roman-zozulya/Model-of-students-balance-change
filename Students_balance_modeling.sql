with first_payments as
    (SELECT "user_id", 
        MIN(transaction_datetime::date) as first_payment_date
    FROM skyeng_db.payments 
    WHERE status_name = 'success'
    GROUP BY 1),
    
/*Step 2
Getting a table with dates for each calendar day of 2016.
*/
all_dates as 
    (SELECT DISTINCT class_start_datetime::date as calendar_dates 
    FROM skyeng_db.classes
    WHERE date_part('year', class_start_datetime) = '2016'), 

/*Step 3 
Find out for which dates it makes sense to collect the balance for each student. 
We will join tables with the first payments and all calendar dates of 2016, 
and create the CTE, which will store all the students' life days after their first transaction has occurred.
*/
all_dates_by_user as 
    (SELECT fp.user_id, 
        ad.calendar_dates
    FROM first_payments as fp 
        INNER JOIN all_dates as ad 
            ON ad.calendar_dates >= fp.first_payment_date),

/*Step 4
Find all balance changes related to successful payment transactions.
*/
payments_by_dates as 
    (SELECT "user_id", 
        transaction_datetime::date as payment_date,
        SUM(coalesce(classes, 0)) as transaction_balance_change
    FROM skyeng_db.payments
    WHERE status_name = 'success'
    GROUP BY 1, 2), 
    
/*Step 5
Let's find the student balances, which are formed only by payment transactions.
Left join tables with all calendar dates of 2016 and balance changes from successful payment transactions. 
Get the cumulative sum of payment transactions balance change for all rows including the current one.
*/
payments_by_dates_cumsum as 
    (SELECT adbu.user_id, 
        adbu.calendar_dates, 
        pbd.transaction_balance_change,
        SUM(coalesce(transaction_balance_change, 0)) 
                OVER(PARTITION BY adbu.user_id 
                ORDER BY adbu.calendar_dates 
                rows between unbounded preceding and current row) as transaction_balance_change_cumsum
    FROM all_dates_by_user as adbu 
        LEFT JOIN payments_by_dates as pbd 
            ON adbu.user_id = pbd.user_id AND adbu.calendar_dates = pbd.payment_date), 

/*Step 6
Find balance changes related to classes completion. 
We are not interested in trial classes and the ones with statuses different from 'success', 'failed_by_student'. 
To reflect that these changes are write-offs, let's multiply the class count by -1.
*/        
classes_by_dates as 
    (SELECT "user_id", 
        class_start_datetime::date as class_date, 
        -1 * COUNT(class_status) as classes --classes completed on a particular day
    FROM skyeng_db.classes 
    WHERE date_part('year', class_start_datetime) = '2016'
        AND class_type != 'trial'
        AND class_status IN ('success', 'failed_by_student')
    GROUP BY 1, 2), 
    
/*Step 7
Left join tables with all calendar dates of 2016 and classes by days 
and get the cumulative sum of class write-offs.
*/
classes_by_dates_dates_cumsum as 
    (SELECT adbu.user_id, 
        adbu.calendar_dates, 
        cbd.classes, 
        SUM(coalesce(classes,0)) --fill nans with 0
                OVER(PARTITION BY adbu.user_id ORDER BY adbu.calendar_dates) as classes_cumsum
    FROM all_dates_by_user as adbu 
        LEFT JOIN classes_by_dates as cbd 
            ON adbu.calendar_dates = cbd.class_date AND adbu.user_id = cbd.user_id), 
        
/*Step 8
Join tables with cumulative sums of balance changes with payments transactions and write-offs from classes completed.
*/
balances as 
    (SELECT pbdcs.user_id, 
        pbdcs.calendar_dates, 
        pbdcs.transaction_balance_change, 
        pbdcs.transaction_balance_change_cumsum, 
        cbddcs.classes, 
        cbddcs.classes_cumsum, 
        cbddcs.classes_cumsum + pbdcs.transaction_balance_change_cumsum as balance 
    FROM payments_by_dates_cumsum as pbdcs 
        INNER JOIN classes_by_dates_dates_cumsum cbddcs 
            ON pbdcs.user_id = cbddcs.user_id AND pbdcs.calendar_dates = cbddcs.calendar_dates)
            
/*Step 9 
Let's see how the total number of classes changed on the students balance.
*/
SELECT calendar_dates as "date", 
    SUM(transaction_balance_change) as transactions_balance_replenishment, 
    SUM(transaction_balance_change_cumsum) as classes_balance_replenishment, 
    SUM(classes) as classes_completion, 
    SUM(classes_cumsum) as classes_balance_writeoffs, 
    SUM(balance) as classes_balance 
FROM balances 
GROUP BY 1
ORDER BY 1

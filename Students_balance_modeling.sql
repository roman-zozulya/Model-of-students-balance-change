/*Step 1
Find out the first successful transaction date for each student. 
Starting from this date, we will collect the balance of their classes.
*/
with first_payments as
    (select user_id, 
        min(transaction_datetime::date) as first_payment_date
    from skyeng_db.payments 
    where status_name = 'success'
    group by 1),
    
/*Step 2
Getting a table with dates for each calendar day of 2016.
*/
all_dates as 
    (select distinct class_start_datetime::date as calendar_dates 
    from skyeng_db.classes
    where date_part('year', class_start_datetime) = '2016'), 

/*Step 3 
Find out for which dates it makes sense to collect the balance for each student. 
We will join tables with the first payments and all calendar dates of 2016, 
and create the CTE, which will store all the students' life days after their first transaction has occurred.
*/
all_dates_by_user as 
    (select fp.user_id, 
        ad.calendar_dates
    from first_payments as fp 
        join all_dates as ad 
            on ad.calendar_dates >= fp.first_payment_date),

/*Step 4
Find all balance changes related to successful payment transactions.
*/
payments_by_dates as 
    (select user_id, 
        transaction_datetime::date as payment_date,
        sum(coalesce(classes, 0)) as transaction_balance_change
    from skyeng_db.payments
    where status_name = 'success'
    group by 1, 2), 
    
/*Step 5
Let's find the student balances, which are formed only by payment transactions.
Left join tables with all calendar dates of 2016 and balance changes from successful payment transactions. 
Get the cumulative sum of payment transactions balance change for all rows including the current one.
*/
payments_by_dates_cumsum as 
    (select adbu.user_id, 
        adbu.calendar_dates, 
        pbd.transaction_balance_change,
        sum(coalesce(transaction_balance_change, 0)) 
                over(partition by adbu.user_id 
                order by adbu.calendar_dates 
                rows between unbounded preceding and current row) as transaction_balance_change_cumsum
    from all_dates_by_user as adbu 
        left join payments_by_dates as pbd 
            on adbu.user_id = pbd.user_id and adbu.calendar_dates = pbd.payment_date), 

/*Step 6
Find balance changes related to classes completion. 
We are not interested in trial classes and the ones with statuses different from 'success', 'failed_by_student'. 
To reflect that these changes are write-offs, let's multiply the class count by -1.
*/        
classes_by_dates as 
    (select user_id, 
        class_start_datetime::date as class_date, 
        -1 * count(class_status) as classes --lessons completed at this day
    from skyeng_db.classes 
    where date_part('year', class_start_datetime) = '2016'
        and class_type != 'trial'
        and class_status in ('success', 'failed_by_student')
    group by 1, 2), 
    
/*Step 7
Left join tables with all calendar dates of 2016 and classes by days 
and get the cumulative sum of class write-offs.
*/
classes_by_dates_dates_cumsum as 
    (select adbu.user_id, 
        adbu.calendar_dates, 
        cbd.classes, 
        sum(coalesce(classes,0)) --fill nans with 0
                over(partition by adbu.user_id order by adbu.calendar_dates) as classes_cumsum
    from all_dates_by_user as adbu 
        left join classes_by_dates as cbd 
            on adbu.calendar_dates = cbd.class_date and adbu.user_id = cbd.user_id), 
        
/*Step 8
Join tables with cumulative sums of balance changes with payments transactions and write-offs from classes completed.
*/
balances as 
    (select pbdcs.user_id, 
        pbdcs.calendar_dates, 
        pbdcs.transaction_balance_change, 
        pbdcs.transaction_balance_change_cumsum, 
        cbddcs.classes, 
        cbddcs.classes_cumsum, 
        cbddcs.classes_cumsum + pbdcs.transaction_balance_change_cumsum as balance 
    from payments_by_dates_cumsum as pbdcs 
        inner join classes_by_dates_dates_cumsum cbddcs 
            on pbdcs.user_id = cbddcs.user_id and pbdcs.calendar_dates = cbddcs.calendar_dates)
            
/*Step 9 
Let's see how the total number of classes changed on the students balance.
*/
select calendar_dates as "date", 
    sum(transaction_balance_change) as sum_transaction_balance_change, 
    sum(transaction_balance_change_cumsum) as sum_transaction_balance_change_cumsum, 
    sum(classes) as sum_classes, 
    sum(classes_cumsum) as sum_classes_cumsum, 
    sum(balance) as sum_balance 
from balances 
group by 1
order by 1

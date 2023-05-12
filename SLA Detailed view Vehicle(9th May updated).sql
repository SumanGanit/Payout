    select
    distinct pc_code, 
    sysdate as timestamp, 
    sla_colour,
    com.payables as commercial_amount,
    case
    when sla_colour = 'RED' then -5 
    else 0 
    end as payables_percentage,
    (commercial_amount * payables_percentage) / 100 as sla_paid, 
    CASE 
    WHEN payables_percentage > 0 THEN 'INCENTIVE' 
    WHEN payables_percentage < 0 THEN 'PENALTY' 
    ELSE Null 
    END as payout_type, 
    from 
    commercial_amount + sla_paid as total_payables (
        select 
        sl.pc_code,
        sl.category, 
        case
        when fad <= 88 
        and td <= 90 then 'RED' 
        else 'GREEN' 
        end as sla_colour 
        from 
        ( 
            select 
            distinct pc_code,
            CASE 
            when td_ecom is not Null
            and td_nonecom is Null THEN 'E-commerce Shipments' 
            when td_ecom is Null 
            and td_nonecom is not Null THEN 'Non-Ecommerce Shipments' 
            When td_ecom is not Null 
            and td_nonecom is not Null THEN 'Combined' 
            end as category,
            CASE
            When category = 'E-commerce Shipments' then fad_ecom
            When category = 'Non-Ecommerce Shipments' then fad_nonecom 
            When category = 'Combined' then ((fad_ecom + fad_nonecom) / 2) 
            end as fad,
            CASE 
            When category = 'E-commerce Shipments' then td_ecom 
            When category = 'Non-Ecommerce Shipments' then td_nonecom 
            When category = 'Combined' then ((td_ecom + td_nonecom) / 2) 
            end as td 
            from 
            dev_payout_report.sla_table 
            inner join dev_payout_report.cluster_master cm on pc_code = cm.vendor_code 
            where 
            cm.type = 'Internal' 
            ) sl 
            ) slf --Commercial Inner Join 
            inner join ( 
                select 
                vendor_code,
                paymonth, 
                sum(payables) as payables 
                from 
                ( 
                    ( 
                        select 
                        vendor_code, 
                        pay_out_month as paymonth,
                        sum(payable_amount) as payables 
                        from 
                        dev_payout_report.vehicle_summary_view_test 
                        where
                        payout_type = 'Commercials' 
                        group by 
                        vendor_code,
                        paymonth 
                        ) 
                ) 
                        group by 
                        vendor_code,
                        paymonth
                            ) com on com.vendor_code = slf.pc_code
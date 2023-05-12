;

with recursive all_dates(dt) as (
    -- anchor
    select
        cast('2022-07-01' as date) dt
    union
    all -- recursion with stop condition
    select
        cast((dt + interval '1 day') as date)
    from
        all_dates
    where
        cast((dt + interval '1 day') as date) <= current_date
) (
    select
        distinct si.pc_code,
        si.biker_code,
        si.vendor_category,
        si.pay_out_month,
        si.model_type,
        round(((si.no_sunday_incentive / sunday_count :: float) * 1000), 0) as sunday_incentive,
        case
            when cnc.delivery_failure = 0 then 1000
            else 0
        end as consignment_closure_incentive,
        sunday_incentive + consignment_closure_incentive as total_incentives
    from
        (
            select
                distinct pc_code,
                biker_code,
                vendor_category,
                pay_out_month,
                model as model_type,
                month_week,
                sunday_count,
                sum(
                    CASE
                        WHEN sunday_incentive = 1 THEN 1
                        ELSE 0
                    END
                ) as no_sunday_incentive
            from
                (
                    select
                        distinct pc_code,
                        biker_code,
                        vendor_category,
                        pay_out_month,
                        rank,
                        year_week_number,
                        model,
                        CASE
                            WHEN weekdays = week_days THEN 1
                            ELSE 0
                        END as attendance_incentive,
                        CASE
                            WHEN weekends = 1 THEN 1
                            ELSE 0
                        END as sunday_incentive
                    from
                        (
                            select
                                distinct pc_code,
                                biker_code,
                                vendor_category,
                                pay_out_month,
                                year_week_number,
                                week_num,
                                RANK() OVER(
                                    PARTITION BY biker_code
                                    ORDER BY
                                        week_num DESC
                                ) as rank,
                                model,
                                sum (
                                    CASE
                                        WHEN weekday > 0 THEN 1
                                        ELSE 0
                                    END
                                ) as weekdays,
                                sum (
                                    CASE
                                        WHEN weekday = 0 THEN 1
                                        ELSE 0
                                    END
                                ) as weekends
                            from
                                (
                                    select
                                        distinct bm.pc_code as pc_code,
                                        bm.biker_code as biker_code,
                                        bm.pcc_master as vendor_category,
                                        cast(
                                            (
                                                to_timestamp(zpdn.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS')
                                            ) as date
                                        ) as delivery_date,
                                        date_part(month, delivery_date) as pay_out_month,
                                        CASE
                                            WHEN (
                                                datepart(year, bm.actvtn_date -1) = datepart(year, delivery_date)
                                                and datepart(month, bm.actvtn_date -1) <> datepart(month, delivery_date)
                                            )
                                            or (
                                                datepart(year, bm.actvtn_date -1) < datepart(year, delivery_date)
                                            ) THEN 'Yes'
                                            ELSE 'No'
                                        END as available_for_the_complete_month,
                                        date_part(w, delivery_date) as year_week_number,
                                        last_day(delivery_date) as last_date,
                                        dateadd(month, -1, last_date) as last_month_date,
                                        date_part(w, last_month_date) as last_month_week_number,
                                        year_week_number - last_month_week_number as week_num,
                                        date_part(dayofweek, delivery_date) as weekday,
                                        CASE
                                            WHEN upper(bm.associate) in ('NO', 'NA')
                                            and upper(bm.attached_to) in ('NO', 'NA') THEN 'BIKER'
                                            WHEN upper(bm.associate) not in ('NO', 'NA')
                                            and upper(bm.attached_to) not in ('NO', 'NA') THEN 'VEHICLE WITH DELIVERY BOY'
                                            WHEN upper(bm.associate) not in ('NO', 'NA')
                                            and upper(bm.attached_to) in ('NO', 'NA') THEN 'VEHICLE WITHOUT DELIVERY BOY'
                                        END as model
                                    from
                                        dev_payout_report.zpdn_table_payout zpdn
                                        inner join dev_payout_report.biker_code_test bc on zpdn.zzconsg_number = bc.reference_number
                                        inner join dev_payout_report.biker_master bm on bm.biker_code = bc.worker_code
                                        inner join dev_payout_report.cluster_master cm on bm.pc_code = cm.vendor_code
                                    where
                                        zpdn.zzcons_status = 'DRS'
                                        and available_for_the_complete_month = 'Yes'
                                        and cm.type = 'Internal'
                                        and (model = 'VEHICLE WITH DELIVERY BOY' OR model= 'VEHICLE WITHOUT DELIVERY BOY')
                                )
                            group by
                                pc_code,
                                biker_code,
                                vendor_category,
                                pay_out_month,
                                model,
                                week_num,
                                year_week_number
                        )
                        left join(
                            select
                                pay_month,
                                sum (
                                    CASE
                                        WHEN week_day > 0 THEN 1
                                        ELSE 0
                                    END
                                ) as week_days,
                                year_week_numbers
                            from
                                (
                                    select
                                        dt,
                                        date_part(month, dt) as pay_month,
                                        date_part(w, dt) as year_week_numbers,
                                        date_part(dayofweek, dt) as week_day
                                    from
                                        all_dates
                                )
                            group by
                                year_week_numbers,
                                pay_month
                        ) dts on dts.year_week_numbers = year_week_number
                        and dts.pay_month = pay_out_month
                )
                left join (
                    select
                        pay_month,
                        max(rank) as month_week,
                        sum(sunday) as sunday_count
                    from
                        (
                            select
                                pay_month,
                                week_num,
                                RANK() OVER(
                                    PARTITION BY pay_month
                                    ORDER BY
                                        week_num DESC
                                ) as rank,
                                sunday
                            from
                                (
                                    select
                                        distinct pay_month,
                                        week_num,
                                        sum(
                                            CASE
                                                WHEN weekday = 0 THEN 1
                                                ELSE 0
                                            END
                                        ) as sunday
                                    from
                                        (
                                            select
                                                dt,
                                                date_part(month, dt) as pay_month,
                                                date_part(w, dt) as year_week_number,
                                                last_day(dt) as last_date,
                                                dateadd(month, -1, last_date) as last_month_date,
                                                date_part(w, last_month_date) as last_month_week_number,
                                                year_week_number - last_month_week_number as week_num,
                                                date_part(dayofweek, dt) as weekday
                                            from
                                                all_dates
                                        )
                                    group by
                                        pay_month,
                                        week_num
                                )
                        )
                    group by
                        pay_month
                ) dtd on dtd.pay_month = pay_out_month
            group by
                pc_code,
                biker_code,
                vendor_category,
                pay_out_month,
                model,
                month_week,
                sunday_count
        ) si
        left join (
            select
                distinct pc_code,
                biker_code,
                pay_out_month,
                sum(
                    case
                        when eligible = 'No' then 1
                        else 0
                    end
                ) as delivery_failure
            from
                (
                    select
                        distinct bm.pc_code as pc_code,
                        bm.biker_code as biker_code,
                        zpdn.zzconsg_number,
                        to_timestamp(zpdn.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS') as deliverydate,
                        to_timestamp(zpdn.handover_datetime, 'DD-MM-YYYY HH24:MI:SS') as handover_date,
                        cast(deliverydate as date) as delivery_date,
                        date_part(month, delivery_date) as pay_out_month,
                        deliverydate - handover_date as closure_time,
                        extract (
                            second
                            from
                                closure_time
                        ) as closure_diff,
                        case
                            when closure_diff <= 86400 then 'Yes'
                            else 'No'
                        end as eligible
                    from
                        dev_payout_report.zpdn_table_payout zpdn
                        inner join dev_payout_report.biker_code_test bc on zpdn.zzconsg_number = bc.reference_number
                        inner join dev_payout_report.biker_master bm on bm.biker_code = bc.worker_code
                        inner join dev_payout_report.cluster_master cm on bm.pc_code = cm.vendor_code
                    where
                        zpdn.zzcons_status = 'DRS'
                        and cm.type = 'Internal'
                )
            group by
                pc_code,
                biker_code,
                pay_out_month
        ) cnc on cnc.biker_code = si.biker_code
        and cnc.pay_out_month = si.pay_out_month
        and cnc.pc_code = si.pc_code
)
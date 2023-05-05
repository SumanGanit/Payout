(
    (
        select
            distinct pc_code,
            vendor_category,
            biker_code,
            onboarding_date,
            pay_out_month,
            pay_out_year,
            model,
            count(distinct delivery_date) as no_of_days_delivered,
            count(distinct consignment) as total_consignments,
            round(
                (
                    total_consignments / no_of_days_delivered :: float
                ),
                0
            ) as productivity,
            (
                ceiling(
                    ((cast (no_of_days_delivered as float)) * 100) /(cast (no_of_total_working_days as float))
                )
            ) as attendance,
            no_of_total_working_days,
            available_for_the_complete_month,
            case
                when (
                    attendance >= 90
                    and productivity >= 30
                    and productivity <= 40
                    and cm_type = 'Internal'
                ) then 15000
                when (
                    attendance >= 90
                    and productivity > 40
                    and cm_type = 'Internal'
                ) then 18000
                when (
                    attendance >= 80
                    and cm_type = 'External'
                ) then coalesce(guaranteed_amount, 0)
                else 0
            end as guaranteedamount,
            sum(rate_per_shipment) as total_commercials,
            CASE
                WHEN (
                    available_for_the_complete_month = 'Yes'
                    and coalesce(guaranteedamount, 0) > total_commercials
                ) THEN guaranteedamount
                WHEN (
                    available_for_the_complete_month = 'No'
                    and coalesce(
                        (
                            (guaranteedamount * no_of_days_delivered) / no_of_working_days_in_a_month
                        ),
                        0
                    ) > total_commercials
                ) THEN (
                    (guaranteedamount * no_of_days_delivered) / no_of_working_days_in_a_month
                )
                ELSE total_commercials
            END as payable_amount,
            payout_type,
            cm_type
        from
            (
                select
                    distinct zp.pc_code,
                    zp.vendor_category,
                    zp.biker_code,
                    zp.onboarding_date,
                    zp.delivery_date,
                    zp.delivery_datetime,
                    zp.handover_datetime,
                    zp.pay_out_month,
                    zp.pay_out_year,
                    zp.pincode,
                    zp.sbu,
                    zp.available_for_the_complete_month,
                    zp.no_of_working_days_in_a_month,
                    zp.no_of_total_working_days,
                    zp.model,
                    zp.charge_weight,
                    zp.consignment,
                    zp.rank,
                    cs.guaranteed_amount_biker as guaranteed_amount,
                    case
                        when zp.cm_type = 'Internal' then cs.slab_rate
                        when zp.cm_type = 'External' then cme.amount
                    end as rate_per_shipment,
                    zp.payout_type,
                    zp.cm_type
                from
                    (
                        (
                            select
                                distinct zpd.pc_code,
                                zpd.vendor_category,
                                zpd.biker_code,
                                zpd.onboarding_date,
                                zpd.delivery_date,
                                zpd.delivery_datetime,
                                zpd.handover_datetime,
                                zpd.pay_out_month,
                                zpd.pay_out_year,
                                zpd.product,
                                zpd.pincode,
                                zpd.sbu,
                                zpd.available_for_the_complete_month,
                                zpd.no_of_working_days_in_a_month,
                                zpd.no_of_total_working_days,
                                zpd.model,
                                zpd.charge_weight,
                                zpd.consignment,
                                RANK() OVER(
                                    PARTITION BY zpd.biker_code,
                                    zpd.delivery_date
                                    ORDER BY
                                        zpd.consignment
                                ) as rank,
                                zpd.payout_type,
                                zpd.pccity,
                                zpd.cm_type
                            from
                                (
                                    select
                                        distinct bm.pc_code as pc_code,
                                        bm.pcc_master as vendor_category,
                                        bm.biker_code as biker_code,
                                        bm.actvtn_date as onboarding_date,
                                        cast(
                                            (
                                                to_timestamp(zpdn.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS')
                                            ) as date
                                        ) as delivery_date,
                                        (
                                            to_timestamp(zpdn.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS')
                                        ) as delivery_datetime,
                                        (
                                            to_timestamp(zpdn.handover_datetime, 'DD-MM-YYYY HH24:MI:SS')
                                        ) as handover_datetime,
                                        to_char(cast(delivery_date as date), 'Month') as pay_out_month,
                                        date_part(year, cast(delivery_date as date)) as pay_out_year,
                                        datepart(day, last_day(delivery_date)) as no_of_days_in_month,
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
                                        (
                                            no_of_days_in_month - DATEDIFF(
                                                week,
                                                (last_day(delivery_date) - no_of_days_in_month),
                                                last_day(delivery_date)
                                            )
                                        ) as no_of_working_days_in_a_month,
                                        CASE
                                            WHEN available_for_the_complete_month = 'Yes' THEN (
                                                no_of_days_in_month - DATEDIFF(
                                                    week,
                                                    (last_day(delivery_date) - no_of_days_in_month),
                                                    last_day(delivery_date)
                                                )
                                            )
                                            WHEN available_for_the_complete_month = 'No' THEN (
                                                no_of_days_in_month - datepart(day, bm.actvtn_date) - DATEDIFF(
                                                    week,
                                                    bm.actvtn_date -1,
                                                    last_day(delivery_date)
                                                ) + 1
                                            )
                                        END as no_of_total_working_days,
                                        zpdn.product,
                                        zpdn.zzdest_pincode as pincode,
                                        'Express' as sbu,
                                        CASE
                                            WHEN upper(bm.associate) in ('NO', 'NA')
                                            and upper(bm.attached_to) in ('NO', 'NA') THEN 'BIKER COMMERCIALS'
                                            WHEN upper(bm.associate) not in ('NO', 'NA')
                                            and upper(bm.attached_to) not in ('NO', 'NA') THEN 'VEHICLE WITH DELIVERY BOY COMMERCIALS'
                                            WHEN upper(bm.associate) not in ('NO', 'NA')
                                            and upper(bm.attached_to) in ('NO', 'NA') THEN 'VEHICLE WITHOUT DELIVERY BOY COMMERCIALS'
                                        END as model,
                                        zpdn.zzcharge_weight as charge_weight,
                                        zpdn.zzconsg_number as consignment,
                                        'Commercials' as payout_type,
                                        upper(cm.cluster_city) as pccity,
                                        cm.type as cm_type
                                    from
                                        dev_payout_report.zpdn_table_payout zpdn
                                        inner join dev_payout_report.biker_code_test bc on zpdn.zzconsg_number = bc.reference_number
                                        inner join dev_payout_report.biker_master bm on bm.biker_code = bc.worker_code
                                        inner join dev_payout_report.cluster_master cm on zpdn.gpart = cm.vendor_code
                                    where
                                        model = 'BIKER COMMERCIALS'
                                        and zpdn.zzcons_status = 'DRS'
                                ) zpd
                        ) zp
                        left join (
                            select
                                pc_type,
                                upper(pc_city) as pc_city,
                                guaranteed_amount_biker,
                                no_of_consignments_from as lower_limit,
                                no_of_consignments_to as upper_limit,
                                commercials as slab_rate
                            from
                                dev_payout_report.commercials
                        ) cs on cs.lower_limit <= zp.rank
                        and cs.upper_limit >= zp.rank
                        and cs.pc_city = zp.pccity
                        and cs.pc_type = zp.cm_type
                        left join (
                            select
                                category,
                                vendor_code,
                                doxtype,
                                product_type_it as product_type,
                                from_value,
                                to_value,
                                amount,
                                add_value,
                                add_amount
                            from
                                dev_payout_report.ndv_commercial_master
                            where
                                category in ('PCC - External', 'Kirana', 'Helix')
                                and commercial_category = 'Shipment'
                                and product_type_it = 'All Products'
                                and service_type = 'Delivery'
                        ) cme on cme.vendor_code = zp.pc_code
                        and cme.from_value <= zp.rank
                        and cme.to_value >= zp.rank
                    )
            )
        group by
            pc_code,
            vendor_category,
            biker_code,
            onboarding_date,
            pay_out_month,
            pay_out_year,
            model,
            guaranteed_amount,
            no_of_total_working_days,
            available_for_the_complete_month,
            payout_type,
            no_of_working_days_in_a_month,
            cm_type
    )
    union
    (
        select
            pc_code,
            vendor_category,
            biker_code,
            onboarding_date,
            pay_out_month,
            pay_out_year,
            model,
            0 :: INTEGER as no_of_days_delivered,
            0 :: INTEGER as total_consignments,
            0 :: INTEGER as productivity,
            0 :: INTEGER as attendance,
            0 :: INTEGER as no_of_total_working_days,
            ' ' :: TEXT as available_for_the_complete_month,
            0 :: INTEGER as guaranteed_amount,
            0 :: INTEGER as total_commercials,
            sum(payables) as payable_amount,
            payout_type,
            cm_type
        from
            (
                select
                    pc_code,
                    vendor_category,
                    biker_code,
                    cast(NULL AS Date) as onboarding_date,
                    delivery_date,
                    pay_out_month,
                    pay_out_year,
                    sbu,
                    model,
                    no_of_consignment_number,
                    incentives as rate,
                    (
                        coalesce(c_slab, 0) +(
                            (no_of_consignment_number - new_lower_limit + 1) * incentives
                        )
                    ) as payables,
                    payout_type,
                    cm_type
                from
                    (
                        (
                            select
                                distinct bm.pc_code,
                                bm.biker_code,
                                bm.pcc_master as vendor_category,
                                'Express' as sbu,
                                cast(
                                    (
                                        to_timestamp(zpdn.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS')
                                    ) as date
                                ) as delivery_date,
                                to_char(cast(delivery_date as date), 'Month') as pay_out_month,
                                date_part(year, cast(delivery_date as date)) as pay_out_year,
                                COUNT(DISTINCT(ZZCONSG_NUMBER)) as no_of_consignment_number,
                                CASE
                                    WHEN upper(bm.associate) in ('NO', 'NA')
                                    and upper(bm.attached_to) in ('NO', 'NA') THEN 'BIKER COMMERCIALS'
                                    WHEN upper(bm.associate) not in ('NO', 'NA')
                                    and upper(bm.attached_to) not in ('NO', 'NA') THEN 'VEHICLE WITH DELIVERY BOY COMMERCIALS'
                                    WHEN upper(bm.associate) not in ('NO', 'NA')
                                    and upper(bm.attached_to) in ('NO', 'NA') THEN 'VEHICLE WITHOUT DELIVERY BOY COMMERCIALS'
                                END as model,
                                'HVDI' as payout_type,
                                upper(cm.cluster_city) as pccity,
                                cm.type as cm_type
                            from
                                dev_payout_report.zpdn_table_payout zpdn
                                inner join dev_payout_report.biker_code_test bc on zpdn.zzconsg_number = bc.reference_number
                                inner join dev_payout_report.biker_master bm on bm.biker_code = bc.worker_code
                                inner join dev_payout_report.cluster_master cm on zpdn.gpart = cm.vendor_code
                            where
                                model = 'BIKER COMMERCIALS'
                                and zzcons_status = 'DRS'
                                and cm_type = 'Internal'
                            group by
                                bm.pc_code,
                                bm.pcc_master,
                                bm.biker_code,
                                sbu,
                                delivery_date,
                                pay_out_month,
                                pay_out_year,
                                model,
                                payout_type,
                                pccity,
                                cm_type
                        )
                        left join (
                            select
                                pc_type,
                                pc_city,
                                lower_limit,
                                new_lower_limit,
                                upper_limit,
                                incentives,
                                slab_total,
                                cum_slab_total,
                                Lag(cum_slab_total, 1) OVER(
                                    PARTITION BY pc_city,
                                    pc_type
                                    ORDER BY
                                        upper_limit asc
                                ) AS c_slab
                            from
                                (
                                    select
                                        pc_type,
                                        pc_city,
                                        lower_limit,
                                        new_lower_limit,
                                        upper_limit,
                                        incentives,
                                        slab_total,
                                        SUM(slab_total) OVER(
                                            PARTITION BY pc_city,
                                            pc_type
                                            ORDER BY
                                                upper_limit asc ROWS BETWEEN UNBOUNDED PRECEDING
                                                AND CURRENT ROW
                                        ) AS cum_slab_total
                                    from
                                        (
                                            select
                                                pc_type,
                                                pc_city,
                                                lower_limit,
                                                new_lower_limit,
                                                upper_limit,
                                                incentives,
                                                ((upper_limit - new_lower_limit + 1) * incentives) as slab_total
                                            from
                                                (
                                                    select
                                                        pc_type,
                                                        pc_city,
                                                        lower_limit,
                                                        case
                                                            when rank_no = 2 then (
                                                                select
                                                                    MIN(no_of_consignments_from)
                                                                from
                                                                    dev_payout_report.hvdi_incentive
                                                            )
                                                            else lower_limit
                                                        end as new_lower_limit,
                                                        upper_limit,
                                                        incentives
                                                    from
                                                        (
                                                            select
                                                                pc_type,
                                                                upper(pc_city) as pc_city,
                                                                no_of_consignments_from as lower_limit,
                                                                no_of_consignments_to as upper_limit,
                                                                incentives,
                                                                RANK () OVER (
                                                                    PARTITION BY pc_city,
                                                                    pc_type
                                                                    ORDER BY
                                                                        upper_limit asc
                                                                ) AS rank_no
                                                            from
                                                                dev_payout_report.hvdi_incentive
                                                        )
                                                )
                                        )
                                )
                        ) hvdi on lower_limit <= cast(no_of_consignment_number as int)
                        and upper_limit >= cast(no_of_consignment_number as int)
                        and pc_city = pccity
                        and pc_type = cm_type
                    )
                group by
                    pc_code,
                    vendor_category,
                    biker_code,
                    sbu,
                    delivery_date,
                    pay_out_month,
                    pay_out_year,
                    no_of_consignment_number,
                    model,
                    payout_type,
                    rate,
                    new_lower_limit,
                    c_slab,
                    payables,
                    onboarding_date,
                    cm_type
            )
        group by
            pc_code,
            vendor_category,
            biker_code,
            onboarding_date,
            pay_out_month,
            pay_out_year,
            model,
            no_of_days_delivered,
            total_consignments,
            productivity,
            attendance,
            no_of_total_working_days,
            available_for_the_complete_month,
            guaranteed_amount,
            total_commercials,
            payout_type,
            cm_type
    )
)
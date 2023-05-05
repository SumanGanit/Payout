(
    select
        distinct bm.vehicle as vehicle_number,
        bm.biker_code as vehicle_code,
        cm.vendor_code as vendor_code,
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
        zpdn.zzconsg_number as consignment,
        zpdn.zzcharge_weight as charge_weight,
        0 :: INTEGER as no_of_consignment_number,
        0 :: INTEGER as incentives,
        case
            when cm.type = 'Internal' then (
                case
                    when charge_weight >= vci.lower_limit
                    and charge_weight <= vci.upper_limit then vci.base_rate
                    else vci.base_rate + (
                        (ceiling(cast(charge_weight as float))) - upper_limit
                    ) *(vci.add_rate / vci.add_value) :: float
                end
            )
            when cm.type = 'External' then (
                case
                    when charge_weight >= ncm.from_value
                    and charge_weight <= ncm.to_value then ncm.amount
                    else ncm.amount + (
                        (ceiling(cast(charge_weight as float))) - to_value
                    ) *(ncm.add_amount / ncm.add_value) :: float
                end
            )
        END as payables,
        'Commercials' as payout_type,
        cm.type as cm_type
    from
        dev_payout_report.zpdn_table_payout zpdn
        inner join dev_payout_report.biker_code_test bc on zpdn.zzconsg_number = bc.reference_number
        inner join dev_payout_report.biker_master bm on bm.biker_code = bc.worker_code
        inner join dev_payout_report.cluster_master cm on zpdn.gpart = cm.vendor_code
        left join (
            select
                pc_code as pccode,
                amount_of_weight_from as lower_limit,
                cast(amount_of_weight_to as decimal) as upper_limit,
                commercials as base_rate,
                additional_weight as add_value,
                additional_amount_per_kg as add_rate
            from
                dev_payout_report.vehicle_commercials
        ) vci on lower_limit <= zpdn.zzcharge_weight
        and pccode = bm.pc_code
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
                and commercial_category = 'Weight'
                and product_type_it = 'All Products'
                and service_type = 'Delivery'
        ) ncm on ncm.vendor_code = bm.pc_code
        and ncm.from_value <= zpdn.zzcharge_weight
        and (
            case
                when ncm.add_value is null then ncm.to_value
                else 10000
            end
        ) >= zpdn.zzcharge_weight
    where
        model <> 'BIKER COMMERCIALS'
        and zpdn.zzcons_status = 'DRS'
)
union
(
    select
        vehicle_number,
        vehicle_code,
        vendor_code,
        cast(NULL AS Date) as onboarding_date,
        delivery_date,
        CAST(NULL AS TIMESTAMP) as delivery_datetime,
        CAST(NULL AS TIMESTAMP) as handover_datetime,
        pay_out_month,
        pay_out_year,
        ' ' :: TEXT as product,
        ' ' :: TEXT as pincode,
        sbu,
        model,
        0 :: VARCHAR as consignment,
        ceiling(chargeweight) as charge_weight,
        no_of_consignment_number,
        incentives,
        charge_weight * incentives as payables,
        payout_type,
        cm_type
    from
        (
            (
                select
                    distinct bm.vehicle as vehicle_number,
                    bm.biker_code as vehicle_code,
                    cm.vendor_code as vendor_code,
                    bm.pc_code as pc_code,
                    'Express' as sbu,
                    cast(
                        (
                            to_timestamp(zpdn.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS')
                        ) as date
                    ) as delivery_date,
                    to_char(cast(delivery_date as date), 'Month') as pay_out_month,
                    date_part(year, cast(delivery_date as date)) as pay_out_year,
                    COUNT(DISTINCT(ZZCONSG_NUMBER)) as no_of_consignment_number,
                    sum(zpdn.zzcharge_weight) as chargeweight,
                    CASE
                        WHEN upper(bm.associate) in ('NO', 'NA')
                        and upper(bm.attached_to) in ('NO', 'NA') then 'BIKER COMMERCIALS'
                        WHEN upper(bm.associate) not in ('NO', 'NA')
                        and upper(bm.attached_to) not in ('NO', 'NA') THEN 'Vehicle_with_Delivery_boy'
                        WHEN upper(bm.associate) not in ('NO', 'NA')
                        and upper(bm.attached_to) in ('NO', 'NA') THEN 'Vehicle_without_Delivery_boy'
                    end as model,
                    'HVDI' as payout_type,
                    cm.type as cm_type
                from
                    dev_payout_report.zpdn_table_payout zpdn
                    inner join dev_payout_report.biker_code_test bc on zpdn.zzconsg_number = bc.reference_number
                    inner join dev_payout_report.biker_master bm on bm.biker_code = bc.worker_code
                    inner join dev_payout_report.cluster_master cm on zpdn.gpart = cm.vendor_code
                where
                    model <> 'BIKER COMMERCIALS'
                    and zzcons_status = 'DRS'
                    and cm.type = 'Internal'
                group by
                    bm.vehicle,
                    bm.biker_code,
                    cm.vendor_code,
                    bm.pc_code,
                    sbu,
                    delivery_date,
                    pay_out_month,
                    pay_out_year,
                    model,
                    payout_type,
                    cm.type
            )
            left join (
                select
                    pc_code as pccode,
                    model as ml,
                    no_of_consignments_from as lower_limit,
                    no_of_consignments_to as upper_limit,
                    incentives
                from
                    dev_payout_report.vehicle_hvdi
            ) hvdi on ml = model
            and lower_limit <= cast(no_of_consignment_number as int)
            and upper_limit >= cast(no_of_consignment_number as int)
            and pccode = pc_code
        )
)
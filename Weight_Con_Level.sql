(
    select
        distinct ndv.gpart as vendor_code,
        ndv.vendor_name,
        ndv.consignment_number,
        ndv.zzcharge_weight,
        ndv.dox_type,
        ndv.category_model,
        ndv.delivery_date,
        ndv.handover_date,
        datepart(month, delivery_date) as pay_out_month,
        ndv.pincode,
        ndv.product,
        ndv.eligible,
        ndv.remarks,
        round((case
            when ndv.eligible = 'Yes' then (
                case
                    when ndvcm.amount is null then (
                        case
                            when ndvcmp.add_value is null
                            and ndv.zzcharge_weight >= ndvcmp.from_value
                            and ndv.zzcharge_weight <= ndvcmp.to_value then ndvcmp.amount
                            when ndv.zzcharge_weight >= ndvcmp.from_value
                            and ndv.zzcharge_weight <= ndvcmp.to_value then ndvcmp.amount
                            else ndvcmp.amount + (
                                (
                                    (ceiling(cast (ndv.zzcharge_weight as float))) - ndvcmp.to_value
                                ) *(ndvcmp.add_amount / ndvcmp.add_value) :: float
                            )
                        end
                    )
                    when ndvcm.add_value is null
                    and ndv.zzcharge_weight >= ndvcm.from_value
                    and ndv.zzcharge_weight <= ndvcm.to_value then ndvcm.amount
                    when ndv.zzcharge_weight >= ndvcm.from_value
                    and ndv.zzcharge_weight <= ndvcm.to_value then ndvcm.amount
                    else ndvcm.amount + (
                        (
                            (ceiling(cast (ndv.zzcharge_weight as float))) - ndvcm.to_value
                        ) *(ndvcm.add_amount / ndvcm.add_value) :: float
                    )
                end
            )
            else 0
        end),2) as commercial,
        kpi.fad,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then (greatest(0, kpi.fad_percent * commercial)) / 100 :: float
            else 0
        end as fad_incentive,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then (least(0, kpi.fad_percent * commercial)) / 100 :: float
            else 0
        end as fad_penalty,
        kpi.td,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then (greatest(0, kpi.td_percent * commercial)) / 100 :: float
            else 0
        end as td_incentive,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then (least(0, kpi.td_percent * commercial)) / 100 :: float
            else 0
        end as td_penalty,
        kpi.scd,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then (greatest(0, kpi.scd_percent * commercial)) / 100 :: float
            else 0
        end as scd_incentive,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then (least(0, kpi.scd_percent * commercial)) / 100 :: float
            else 0
        end as scd_penalty,
        case
            when ndv.eligible = 'Yes'
            and commercial > 0 then codr.cod_penalty
            else 0
        end as codpenalty,
        (
            (
                COALESCE(commercial, 0) + fad_incentive + td_incentive + scd_incentive
            ) + (fad_penalty + td_penalty + scd_penalty) - COALESCE(codpenalty, 0)
        ) as net_payable
    from
        (
            select
                distinct ndvp.gpart,
                vm.vendor_name as vendor_name,
                ndvp.zzconsg_number as consignment_number,
                ndvp.zzcharge_weight,
                ndvp.dox_type,
                ndvp.zzcons_status,
                to_timestamp(ndvp.zzdelivery_date, 'DD-MM-YYYY HH24:MI:SS') as delivery_date,
                to_timestamp(ndvp.handover_datetime, 'DD-MM-YYYY HH24:MI:SS') as handover_date,
                delivery_date - handover_date as closure_time,
                extract (
                    second
                    from
                        closure_time
                ) as closure_diff,
                case
                    when closure_diff <= 172800 then 'Yes'
                    else 'No'
                end as eligible,
                case
                    when eligible = 'Yes' then ''
                    else 'Closed after 48 hours'
                end as remarks,
                ndvp.zzdest_pincode as pincode,
                ndvp.product,
                case
                    when ndvcd.category is null then ndvcb.category
                    else ndvcd.category
                end as category_model
            from
                dev_payout_report.ndv_payout ndvp
                inner join dev_payout_report.ndv_vendor_master vm on vm.vendor_code = ndvp.gpart
                left join (
                    select
                        distinct category,
                        doxtype,
                        vendor_code,
                        product_type_it
                    from
                        dev_payout_report.ndv_commercial_master
                ) ndvcd on upper(ndvcd.doxtype) = upper (ndvp.dox_type)
                and ndvcd.vendor_code = ndvp.gpart
                and upper(ndvcd.product_type_it) = upper(ndvp.product)
                left join (
                    select
                        distinct category,
                        doxtype,
                        vendor_code,
                        product_type_it
                    from
                        dev_payout_report.ndv_commercial_master
                    where
                        doxtype = 'Both'
                ) ndvcb on ndvcb.vendor_code = ndvp.gpart
                and upper(ndvcb.product_type_it) = upper(ndvp.product)
            where
                ndvp.zzcons_status = 'DRS'
                and category_model = 'Weight'
                and vm.vendor_category = 'DLV'
        ) ndv
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
                add_amount,
                from_date,
                to_date
            from
                dev_payout_report.ndv_commercial_master
        ) ndvcm on ndvcm.vendor_code = ndv.gpart
        and upper(ndv.dox_type) = upper(ndvcm.doxtype)
        and ndvcm.from_value <= ndv.zzcharge_weight
        and (
            case
                when ndvcm.add_value is null then ndvcm.to_value
                else 10000
            end
        ) >= ndv.zzcharge_weight
        and cast(ndv.delivery_date as date) >= ndvcm.from_date
        and cast(ndv.delivery_date as date) <= ndvcm.to_date
        and upper(ndv.product) = upper(ndvcm.product_type)
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
                add_amount,
                from_date,
                to_date
            from
                dev_payout_report.ndv_commercial_master
            where
                doxtype = 'Both'
        ) ndvcmp on ndvcmp.vendor_code = ndv.gpart
        and ndvcmp.from_value <= ndv.zzcharge_weight
        and (
            case
                when ndvcmp.add_value is null then ndvcmp.to_value
                else 10000
            end
        ) >= ndv.zzcharge_weight
        and cast(ndv.delivery_date as date) >= ndvcmp.from_date
        and cast(ndv.delivery_date as date) <= ndvcmp.to_date
        and upper(ndv.product) = upper(ndvcmp.product_type)
        left join (
            select
                distinct con_number,
                cast(delivery_date as date) as del_date,
                date_part(month, del_date) as pay_month,
                to_timestamp(delivery_date_timestamp, 'DD-MM-YYYY HH24:MI:SS') as time_del_date,
                to_timestamp(utr_datetime, 'DD-MM-YYYY HH24:MI:SS') as time_utr_date,
                time_utr_date - time_del_date as diff,
                extract (
                    second
                    from
                        diff
                ) as time_diff,
                case
                    when time_diff <= 172800 THEN 1
                    ELSE 0
                END as cod_success,
                case
                    when cod_success = 1 then 0
                    else 50
                end as cod_penalty
            from
                dev_payout_report.cod_remittance
            where
                utr_datetime is not null
        ) codr on codr.con_number = ndv.consignment_number
        left join (
            select
                kpit.vendor_code,
                kpit.from_date,
                kpit.to_date,
                kpit.product,
                kpit.fad,
                kpit.td,
                kpit.scd,
                fadm.sla_percent as fad_percent,
                tdm.sla_percent as td_percent,
                scdm.sla_percent as scd_percent
            from
                dev_payout_report.ndv_kpi_table kpit
                left join (
                    select
                        kpi_name,
                        product,
                        sla_from,
                        sla_to,
                        sla_percent,
                        from_date,
                        to_date
                    from
                        dev_payout_report.ndv_kpi_master
                    where
                        kpi_name = 'FAD'
                ) fadm on fadm.sla_from <= kpit.fad
                and fadm.sla_to >= kpit.fad
                and fadm.product = kpit.product
                and fadm.from_date <= kpit.from_date
                and fadm.to_date >= kpit.to_date
                left join (
                    select
                        kpi_name,
                        product,
                        sla_from,
                        sla_to,
                        sla_percent,
                        from_date,
                        to_date
                    from
                        dev_payout_report.ndv_kpi_master
                    where
                        kpi_name = 'TD'
                ) tdm on tdm.sla_from <= kpit.td
                and tdm.sla_to >= kpit.td
                and tdm.product = kpit.product
                and tdm.from_date <= kpit.from_date
                and tdm.to_date >= kpit.to_date
                left join (
                    select
                        kpi_name,
                        product,
                        sla_from,
                        sla_to,
                        sla_percent,
                        from_date,
                        to_date
                    from
                        dev_payout_report.ndv_kpi_master
                    where
                        kpi_name = 'SCD'
                ) scdm on scdm.sla_from <= kpit.scd
                and scdm.sla_to >= kpit.scd
                and scdm.product = kpit.product
                and scdm.from_date <= kpit.from_date
                and scdm.to_date >= kpit.to_date
        ) kpi on kpi.vendor_code = ndv.gpart
        and cast(ndv.delivery_date as date) >= kpi.from_date
        and cast(ndv.delivery_date as date) <= kpi.to_date
        and upper(ndv.product) = upper(kpi.product)
)
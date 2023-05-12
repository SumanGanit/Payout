SELECT DISTINCT
    slf.pc_code,
    SYSDATE AS timestamp,
    slf.sla_colour,
    com.payables AS commercial_amount,
    CASE
        WHEN slf.sla_colour = 'RED' THEN -5
        ELSE 0
    END AS payables_percentage,
    (com.payables * CASE
        WHEN slf.sla_colour = 'RED' THEN -5
        ELSE 0
    END) / 100 AS sla_paid,
    CASE
        WHEN CASE
            WHEN slf.sla_colour = 'RED' THEN -5
            ELSE 0
        END > 0 THEN 'INCENTIVE'
        WHEN CASE
            WHEN slf.sla_colour = 'RED' THEN -5
            ELSE 0
        END < 0 THEN 'PENALTY'
        ELSE NULL
    END AS payout_type
FROM (
    SELECT
        sl.pc_code,
        sl.category,
        CASE
            WHEN sl.fad <= 88 AND sl.td <= 90 THEN 'RED'
            ELSE 'GREEN'
        END AS sla_colour
    FROM (
        SELECT DISTINCT
            pc_code,
            CASE
                WHEN td_ecom IS NOT NULL AND td_nonecom IS NULL THEN 'E-commerce Shipments'
                WHEN td_ecom IS NULL AND td_nonecom IS NOT NULL THEN 'Non-Ecommerce Shipments'
                WHEN td_ecom IS NOT NULL AND td_nonecom IS NOT NULL THEN 'Combined'
            END AS category,
            CASE
                WHEN category = 'E-commerce Shipments' THEN fad_ecom
                WHEN category = 'Non-Ecommerce Shipments' THEN fad_nonecom
                WHEN category = 'Combined' THEN (fad_ecom + fad_nonecom) / 2
            END AS fad,
            CASE
                WHEN category = 'E-commerce Shipments' THEN td_ecom
                WHEN category = 'Non-Ecommerce Shipments' THEN td_nonecom
                WHEN category = 'Combined' THEN (td_ecom + td_nonecom) / 2
            END AS td
        FROM
            dev_payout_report.sla_table
        INNER JOIN dev_payout_report.cluster_master cm ON pc_code = cm.vendor_code
        WHERE
            cm.type = 'Internal'
    ) sl
) slf
INNER JOIN (
    SELECT
        vendor_code,
        paymonth,
        SUM(payables) AS payables
    FROM (
        SELECT
            vendor_code,
            pay_out_month AS paymonth,
            SUM(payable_amount) AS payables
        FROM
            dev_payout_report.vehicle_summary_view_test
        WHERE
            payout_type = 'Commercials'
        GROUP BY
            vendor_code,
            paymonth
    ) sub
    GROUP BY
        vendor_code,
        paymonth
) com ON com.vendor_code = slf.pc_code;

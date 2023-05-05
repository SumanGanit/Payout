select distinct pc_code,
sysdate as timestamp,
category,
sla_final_colour,
com.payables as commercial_amount,
sla.incentive as payables_percentage,
(commercial_amount*payables_percentage)/100 as sla_paid,
CASE WHEN payables_percentage > 0 THEN 'INCENTIVE'
WHEN payables_percentage < 0 THEN 'PENALTY'
ELSE Null
END as payout_type,
commercial_amount+sla_paid as total_payables
from
(select sl.pc_code,
sl.category,
fdt.colour as fad_colour,
tdt.colour as td_colour,
scdt.colour as scd_colour,
CASE WHEN fad_colour = 'Gold' THEN 3
WHEN fad_colour = 'Green' THEN 2
WHEN fad_colour = 'Red' THEN 0
END as fad_number,
CASE WHEN td_colour = 'Gold' THEN 3
WHEN td_colour = 'Green' THEN 2
WHEN td_colour = 'Red' THEN 0
END as td_number,
CASE WHEN scd_colour = 'Gold' THEN 3
WHEN scd_colour = 'Green' THEN 2
WHEN scd_colour = 'Red' THEN 0
END as scd_number,
(fad_number+td_number+scd_number) as sla_number,
CASE WHEN sla_number <= 3 THEN 'Red'
WHEN sla_number >= 8 THEN 'Gold'
WHEN sla_number between 4 and 7 THEN 'Green'
END AS sla_final_colour
from 
(select distinct pc_code,
CASE
when td_ecom is not Null and td_nonecom is Null THEN 'E-commerce Shipments'
when td_ecom is Null and td_nonecom is not Null THEN 'Non-Ecommerce Shipments'
When td_ecom is not Null and td_nonecom is not Null THEN 'Combined'
end as category,
CASE
When category = 'E-commerce Shipments' then fad_ecom
When category = 'Non-Ecommerce Shipments' then fad_nonecom
When category = 'Combined' then ((fad_ecom+fad_nonecom)/2)
end as fad,
CASE
When category = 'E-commerce Shipments' then td_ecom
When category = 'Non-Ecommerce Shipments' then td_nonecom
When category = 'Combined' then  ((td_ecom+td_nonecom)/2)
end as td,
CASE
When category = 'E-commerce Shipments' then scd_ecom
When category = 'Non-Ecommerce Shipments' then scd_nonecom
When category = 'Combined' then  ((scd_ecom+scd_nonecom)/2)
end as scd
from dev_payout_report.sla_table 
inner join dev_payout_report.cluster_master cm on pc_code =cm.vendor_code
where cm.type = 'Internal') sl
left join (select colour,
category,
fad_min,
fad_max 
from dev_payout_report.sla_master_table
where pc_type = 'PCC') fdt on sl.category = fdt.category and sl.fad >= fad_min and sl.fad<= fad_max
left join (select colour,
category,
td_min,
td_max 
from dev_payout_report.sla_master_table
where pc_type = 'PCC') tdt on sl.category = tdt.category and sl.td >= td_min and sl.td<= td_max
left join (select colour,
category,
scd_min,
scd_max 
from dev_payout_report.sla_master_table
where pc_type = 'PCC') scdt on sl.category = scdt.category and sl.scd >= scd_min and sl.scd<= scd_max)
--SLA Final Colour left join
left join (select distinct colour, 
incentive
from dev_payout_report.sla_master_table) sla on sla.colour = sla_final_colour
--Commercial left join
left join (select vendor_code,
paymonth,
sum(payables) as payables
from ((select pc_code as vendor_code,
pay_out_month as paymonth,
sum(payable_amount) as payables
from dev_payout_report.biker_summary_view_test
where payout_type = 'Commercials'
group by vendor_code,
paymonth))
group by vendor_code,
paymonth)com on com.vendor_code = pc_code
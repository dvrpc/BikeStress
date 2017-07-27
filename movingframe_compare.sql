CREATE TABLE movingframe_180_compare_all AS
SELECT 
	t1.edge, 
	t1.count AS original, 
	t2.count AS MF1, 
	t3.count AS MF2,
	t2.count - t1.count AS MF1_O,
	t3.count - t1.count AS MF2_O,
	t3.count - t2.count AS MF2_1,
	t4.geom
FROM public."montco_L3_edgecounts_original180" t1
INNER JOIN public."montco_L3_edgecounts_MFtest" t2
ON t1.edge = t2.edge
INNER JOIN public."montco_L3_edgecounts_180_MF2" t3
ON t1.edge = t3.edge
INNER JOIN public."montco_L3_master_links_grp" t4
ON t1.edge = t4.mixid
ORDER BY edge;
COMMIT;


SELECT
	min(mf1_o) min1,
	max(mf1_o) max1,
	sum(mf1_o) sum1,
	sum(abs(mf1_o)) abssum1,

	min(mf2_o) min2,
	max(mf2_o) max2,
	sum(mf2_o) sum2,
	sum(abs(mf2_o)) abssum2,

	min(mf2_1) min3,
	max(mf2_1) max3,
	sum(mf2_1) sum3,
	sum(abs(mf2_1)) abssum3
FROM public.movingframe_180_compare_all;



--- 196 compare (just 2 tables)

CREATE TABLE movingframe_196_compare2 AS
SELECT 
	t1.edge, 
	t1.count AS original, 
	t2.count AS full, 
    t3.count AS MF,
	t2.count - t1.count AS full_O,
    t3.count - t1.count AS MF_O,
    t3.count - t2.count AS MF_full,
	t4.geom
FROM public."montco_L3_edgecounts" t1
INNER JOIN public."montco_L3_edgecounts_196" t2
ON t1.edge = t2.edge
INNER JOIN public."montco_L3_edgecounts_196_MF" t3
ON t1.edge = t3.edge
INNER JOIN public."montco_L3_master_links_grp" t4
ON t1.edge = t4.mixid
ORDER BY edge;
COMMIT;

SELECT
	min(full_O) min1,
	max(full_O) max1,
	sum(full_O) sum1,
	sum(abs(full_O)) abssum1,
    
	min(MF_O) min1,
	max(MF_O) max1,
	sum(MF_O) sum1,
	sum(abs(MF_O)) abssum1, 
    
	min(MF_full) min1,
	max(MF_full) max1,
	sum(MF_full) sum1,
	sum(abs(MF_full)) abssum1
FROM public.movingframe_196_compare2;
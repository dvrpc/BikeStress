SELECT strong, COUNT(*) as cnt
FROM public."montco_L3_master_links_grp"
GROUP BY strong
ORDER BY cnt DESC;
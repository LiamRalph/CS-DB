select now() AT TIME ZONE 'EST'-'2022-12-27 11:18:00' as timerunning, count(distinct m.mapid) as mapsParsed,  
(now() AT TIME ZONE 'EST'-'2022-12-27 11:18:00')/count(distinct m.mapid) as timePerMap, 
min(ma.date) as latestdate,
(9583-count(distinct m.mapid))*(now() AT TIME ZONE 'EST'-'2022-12-27 11:18:00')/count(distinct m.mapid) as EstTimeRemaining,
now() + ((9583-count(distinct m.mapid))*(now() AT TIME ZONE 'EST'-'2022-12-27 11:18:00')/count(distinct m.mapid)) as EstFinishTime
from matches ma
inner join maps m on m.matchid = ma.matchid
inner join roundstates rs on rs.tick = 0 and rs.round = 1 and rs.mapid = m.mapid
inner join kills K on K.mapid = RS.mapid and K.round = RS.round
having count(K.*) > 0
order by latestdate asc 
limit 1
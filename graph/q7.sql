SELECT distinct g2.src, g2.dst
FROM Graph AS g1, Graph AS g2, Graph AS g3, Graph AS g4, Graph AS g5
WHERE g1.dst = g2.src AND g2.dst = g3.src AND g4.dst = g2.src AND g2.dst = g5.src
SELECT  resources.id as id
FROM    resources JOIN resource_labels AS rl
WHERE   resources.id = rl.id
AND     resources.tenant_id = :tenantId
AND     resources.id IN (
    SELECT      most_inner_rl.id
    FROM        resource_labels AS most_inner_rl
    WHERE       %s
    GROUP BY    most_inner_rl.id
    HAVING COUNT(*) = :i
   )
GROUP BY resources.id
ORDER BY resources.id
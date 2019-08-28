SELECT
   resources.id as id
FROM
   resources JOIN resource_labels AS rl
WHERE
   resources.id = rl.id
   AND resources.id IN
   (
      SELECT id
      FROM resource_labels
      WHERE
         id IN
         (
            SELECT id
            FROM resources
            WHERE tenant_id = :tenantId
         )
         AND resources.id IN
         (
            SELECT id
            FROM resource_labels
            WHERE %s
            GROUP BY id
            HAVING COUNT(*) = :i
         )
   )
ORDER BY resources.id
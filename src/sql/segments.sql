SELECT T1.*,

    CASE WHEN pct_receita <= 0.5 AND pct_freq <= 0.5 THEN 'BAIXO V. BAIXO F.'
        WHEN pct_receita > 0.5 AND pct_freq <= 0.5 THEN 'ALTO VALOR'
        WHEN pct_receita <= 0.5 AND pct_freq > 0.5 THEN 'ALTO FREQ'
        WHEN pct_receita < 0.9 OR pct_freq < 0.9 THEN 'PRODUTIVO'
        ELSE 'SUPER PRODUTIVO'
    END AS SEGMENTO_VALOR_FREQ,

    CASE WHEN qtde_dias_base <= 60 THEN 'INICIO'
        WHEN qtde_dias_ult_venda >=300 THEN 'RETENCAO'
        ELSE 'ATIVO'
    END AS SEGMENTO_VIDA,

    '2018-06-01' AS dt_sgmt

FROM (

    SELECT T1.*,
        PERCENT_RANK() OVER (ORDER BY receita_total ASC) AS pct_receita,
        PERCENT_RANK() OVER (ORDER BY qtde_pedidos ASC) AS pct_freq  

    FROM (
     
        SELECT T2.seller_id,
                SUM( T2.price) AS receita_total,
                COUNT(DISTINCT T1.order_id ) AS qtde_pedidos,
                COUNT( T2.product_id ) AS qtde_produtos,
                COUNT(DISTINCT T2.product_id ) AS qtde_produtos,
                MIN( CAST(julianday('2018-06-01') - julianday(T1.order_approved_at) AS INT) ) AS qtde_dias_ult_venda,
                MAX( CAST(julianday('2018-06-01') - julianday( dt_inicio) AS INT) ) AS qtde_dias_base
    
        FROM tb_orders AS T1

        LEFT JOIN tb_order_items AS T2
        ON T1.order_id = T2.order_id

        LEFT JOIN (
            SELECT T2.seller_id,
                    MIN( DATE( T1.order_approved_at ) ) AS dt_inicio
            FROM tb_orders AS T1
            LEFT JOIN tb_order_items AS T2
            ON T1.order_id = T2.order_id
            GROUP BY T2.seller_id
        ) AS T3

        ON T2.seller_id = T3.seller_id
        WHERE T1.order_approved_at BETWEEN '2017-06-01' AND '2018-06-01'
        GROUP BY T2.seller_id

    ) AS T1
) AS T1
WHERE seller_id IS NOT NULL
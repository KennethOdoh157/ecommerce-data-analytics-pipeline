EXEC Silver.load_silver
/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    BEGIN TRY
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';
        -- Loading silver.olist_geolocation_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_geolocation_dataset';
        TRUNCATE TABLE silver.olist_geolocation_dataset;
        PRINT '>> Inserting Data Into: silver.olist_geolocation_dataset';
        WITH base AS (
            SELECT 
                geolocation_zip_code_prefix,
                TRY_CAST(geolocation_lat AS FLOAT) AS lat,
                TRY_CAST(geolocation_lng AS FLOAT) AS lng,
                TRIM(geolocation_city) AS city_raw,
                UPPER(TRIM(geolocation_state)) AS state_raw
            FROM bronze.olist_geolocation_dataset
            WHERE geolocation_zip_code_prefix IS NOT NULL
              AND geolocation_zip_code_prefix BETWEEN 1000 AND 99999
              AND TRY_CAST(geolocation_lat AS FLOAT) BETWEEN -90 AND 90
              AND TRY_CAST(geolocation_lng AS FLOAT) BETWEEN -180 AND 180
        ),

        city_counts AS (
            SELECT 
                geolocation_zip_code_prefix,
                city_raw,
                COUNT(*) AS cnt
            FROM base
            GROUP BY geolocation_zip_code_prefix, city_raw
        ),

        state_counts AS (
            SELECT 
                geolocation_zip_code_prefix,
                state_raw,
                COUNT(*) AS cnt
            FROM base
            GROUP BY geolocation_zip_code_prefix, state_raw
        ),

        best_city AS (
            SELECT geolocation_zip_code_prefix, city_raw
            FROM (
                SELECT *,
                    ROW_NUMBER() OVER (
                        PARTITION BY geolocation_zip_code_prefix
                        ORDER BY cnt DESC, city_raw
                    ) AS rn
                FROM city_counts
            ) t
            WHERE rn = 1
        ),

        best_state AS (
            SELECT geolocation_zip_code_prefix, state_raw AS state_clean
            FROM (
                SELECT 
                    geolocation_zip_code_prefix,
                    state_raw,
                    cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY geolocation_zip_code_prefix
                        ORDER BY cnt DESC, state_raw ASC
                    ) AS rn
                FROM state_counts
            ) t
            WHERE rn = 1
        ),

        avg_coords AS (
            SELECT 
                geolocation_zip_code_prefix,
                ROUND(AVG(lat), 6) AS lat_clean,
                ROUND(AVG(lng), 6) AS lng_clean
            FROM base
            GROUP BY geolocation_zip_code_prefix
        )

        INSERT INTO silver.olist_geolocation_dataset (
            geolocation_zip_code_prefix,
            geolocation_lat,
            geolocation_lng,
            geolocation_city,
            geolocation_state
        )
        SELECT 
            a.geolocation_zip_code_prefix,
            a.lat_clean AS geolocation_lat,
            a.lng_clean AS geolocation_lng,
            REPLACE(
            CASE
                WHEN LOWER(TRIM(bc.city_raw)) IN ('* cidade') THEN NULL
                WHEN LOWER(TRIM(bc.city_raw)) IN ('...arraial do cabo') THEN 'Arraial do Cabo'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('´teresopolis') THEN 'Teresópolis'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('4º centenario','4o. centenario') THEN 'Quarto Centenário'
                WHEN LOWER(TRIM(bc.city_raw)) = 'alta floresta d''oeste' THEN 'Alta Floresta d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'alvorada d''oeste' THEN 'Alvorada d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'antunes (igaratinga)' THEN 'Antunes (Igaratinga)'
                WHEN LOWER(TRIM(bc.city_raw)) = 'aparecida d''oeste' THEN 'Aparecida d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'apicum-acu' THEN 'Apicum-Açu'
                WHEN LOWER(TRIM(bc.city_raw)) = 'arco-iris' THEN 'Arco-Íris'
                WHEN LOWER(TRIM(bc.city_raw)) = 'arraial d''ajuda' THEN 'Arraial d''Ajuda'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'bacaxa%' THEN 'Bacaxá (Saquarema)'
                WHEN LOWER(TRIM(bc.city_raw)) = 'bandeirantes d''oeste' THEN 'Bandeirantes d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('barra d''alcantara','barra d''alcântara') THEN 'Barra d''Alcântara'
                WHEN LOWER(TRIM(bc.city_raw)) = 'biritiba-mirim' THEN 'Biritiba-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'california da barra%' THEN 'Califórnia da Barra (Barra do Piraí)'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'campo alegre de lourdes%' THEN 'Campo Alegre de Lourdes'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('ceara-mirim','ceará-mirim') THEN 'Ceará-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('colonia z-3','colônia z-3') THEN 'Colônia Z-3 (Pelotas)'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('conceicao do lago-acu','conceição do lago-açu') THEN 'Conceição do Lago-Açu'
                WHEN LOWER(TRIM(bc.city_raw)) = 'conquista d''oeste' THEN 'Conquista d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'diamante d''oeste' THEN 'Diamante d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('dias d''avila','dias d''ávila') THEN 'Dias d''Ávila'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('embu-guacu','embu-guaçu') THEN 'Embu-Guaçu'
                WHEN LOWER(TRIM(bc.city_raw)) = 'encantado d''oeste' THEN 'Encantado d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('entre-ijuis','entre-ijuís') THEN 'Entre-Ijuís'
                WHEN LOWER(TRIM(bc.city_raw)) = 'estrela d''oeste' THEN 'Estrela d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('figueiropolis d''oeste','figueirópolis d''oeste') THEN 'Figueirópolis d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'florian&oacute;polis' THEN 'Florianópolis'
                WHEN LOWER(TRIM(bc.city_raw)) = 'gloria d''oeste' THEN 'Glória d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'governador dix-sept rosado' THEN 'Governador Dix-Sept Rosado'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('guajara-mirim','guajará-mirim') THEN 'Guajará-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) = 'guarani d''oeste' THEN 'Guarani d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'guarda-mor' THEN 'Guarda-Mor'
                WHEN LOWER(TRIM(bc.city_raw)) = 'guarulhos-sp' THEN 'Guarulhos'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('herval d'' oeste','herval d''oeste') THEN 'Herval d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('igarape-acu','igarapé-açu') THEN 'Igarapé-Açu'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('igarape-miri','igarapé-miri') THEN 'Igarapé-Miri'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'itabatan%' THEN 'Itabatan (Mucuri)'
                WHEN LOWER(TRIM(bc.city_raw)) = 'itapecuru-mirim' THEN 'Itapecuru-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) = 'itapejara d''oeste' THEN 'Itapejara d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'itaporanga d''ajuda' THEN 'Itaporanga d''Ajuda'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'jacare %' THEN 'Jacaré (Cabreúva)'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('ji-parana','ji-paraná') THEN 'Ji-Paraná'
                WHEN LOWER(TRIM(bc.city_raw)) = 'lagoa d''anta' THEN 'Lagoa d''Anta'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'lambari%' THEN 'Lambari d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'maceia³' THEN 'Maceió'
                WHEN LOWER(TRIM(bc.city_raw)) = 'machadinho d''oeste' THEN 'Machadinho d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('mae d''agua','mãe d''água') THEN 'Mãe d''Água'
                WHEN LOWER(TRIM(bc.city_raw)) = 'mirassol d''oeste' THEN 'Mirassol d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'mogi-guacu' THEN 'Mogi-Guaçu'
                WHEN LOWER(TRIM(bc.city_raw)) = 'mogi-mirim' THEN 'Mogi-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'monte gordo%' THEN 'Monte Gordo (Camaçari)'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('nao-me-toque','não-me-toque') THEN 'Não-Me-Toque'
                WHEN LOWER(TRIM(bc.city_raw)) = 'naque-nanuque' THEN 'Naque'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('nova brasilandia d''oeste','nova brasilândia d''oeste') THEN 'Nova Brasilândia d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('olho d''agua','olho d''água') THEN 'Olho d''Água'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('olho d''agua das cunhas','olho d''água das cunhãs') THEN 'Olho d''Água das Cunhãs'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('olho d''agua das flores','olho d''água das flores') THEN 'Olho d''Água das Flores'
                WHEN LOWER(TRIM(bc.city_raw)) = 'olho d''água do casado' THEN 'Olho d''Água do Casado'
                WHEN LOWER(TRIM(bc.city_raw)) = 'olho d''água do piauí' THEN 'Olho d''Água do Piauí'
                WHEN LOWER(TRIM(bc.city_raw)) = 'olho d''agua grande' THEN 'Olho d''Água Grande'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'olho-d%borges%' THEN 'Olho d''Água do Borges'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('olhos d''agua','olhos d''água') THEN 'Olhos d''Água'
                WHEN LOWER(TRIM(bc.city_raw)) = 'olhos d''agua do oeste' THEN 'Olhos d''Água do Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'palmeira d''oeste' THEN 'Palmeira d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'paraná d''oeste' THEN 'Paraná d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('pariquera-acu','pariquera-açu') THEN 'Pariquera-Açu'
                WHEN LOWER(TRIM(bc.city_raw)) = 'pau d''arco' THEN 'Pau d''Arco'
                WHEN LOWER(TRIM(bc.city_raw)) = 'peixe-boi' THEN 'Peixe-Boi'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'penedo%' THEN 'Penedo (Itatiaia)'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('perola d''oeste','pérola d''oeste') THEN 'Pérola d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'pindaré-mirim' THEN 'Pindaré-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('pingo-d agua','pingo-d''agua') THEN 'Pingo-d''Água'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'praia grande%' THEN 'Praia Grande (Fundão)'
                WHEN LOWER(TRIM(bc.city_raw)) = 'quilometro 14 do mutum' THEN 'Quilômetro 14 do Mutum'
                WHEN LOWER(TRIM(bc.city_raw)) = 'rancho alegre d''oeste' THEN 'Rancho Alegre d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'realeza%' THEN 'Realeza (Manhuaçu)'
                WHEN LOWER(TRIM(bc.city_raw)) = 'riacho fundo 2' THEN 'Riacho Fundo II'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'rio de janeiro%' THEN 'Rio de Janeiro'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('sa£o paulo') THEN 'São Paulo'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('santa bárbara d`oeste','santa barbara d''oeste','santa bárbara d''oeste') THEN 'Santa Bárbara d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'santa clara d''oeste' THEN 'Santa Clara d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'santa luzia d''oeste' THEN 'Santa Luzia d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'santa rita d''oeste' THEN 'Santa Rita d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) = 'sant''ana do livramento' THEN 'Santana do Livramento'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('sao felipe d''oeste','são felipe d''oeste') THEN 'São Felipe d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('sao joao d''alianca','são joão d''aliança') THEN 'São João d''Aliança'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'são joão do pau%' THEN 'São João do Pau d''Alho'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('sao jorge d''oeste','são jorge d''oeste') THEN 'São Jorge d''Oeste'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('sapucai-mirim','sapucaí-mirim') THEN 'Sapucaí-Mirim'
                WHEN LOWER(TRIM(bc.city_raw)) = 'sitio d''abadia' THEN 'Sítio d''Abadia'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'tamoios%' THEN 'Tamoios (Cabo Frio)'
                WHEN LOWER(TRIM(bc.city_raw)) = 'tanque d''arca' THEN 'Tanque d''Arca'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('tome-acu','tomé-açu','tomé-açú') THEN 'Tomé-Açu'
                WHEN LOWER(TRIM(bc.city_raw)) = 'varre-sai' THEN 'Varre-Sai'
                WHEN LOWER(TRIM(bc.city_raw)) = 'vau-acu' THEN 'Vau-Açu'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'venda nova%' THEN 'Venda Nova do Imigrante'
                WHEN LOWER(TRIM(bc.city_raw)) = 'venha-ver' THEN 'Venha-Ver'
                WHEN LOWER(TRIM(bc.city_raw)) LIKE 'vitorinos%' THEN 'Vitorinos (Alto Rio Doce)'
                WHEN LOWER(TRIM(bc.city_raw)) IN ('xangri-la','xangri-lá') THEN 'Xangri-Lá'
                WHEN LOWER(TRIM(bc.city_raw)) = 'xique-xique' THEN 'Xique-Xique'
                ELSE bc.city_raw
            END,
            'sao',
            'São'
            ) AS geolocation_city,
            bs.state_clean AS geolocation_state
        FROM avg_coords a
        LEFT JOIN best_city  bc ON a.geolocation_zip_code_prefix = bc.geolocation_zip_code_prefix
        LEFT JOIN best_state bs ON a.geolocation_zip_code_prefix = bs.geolocation_zip_code_prefix
        ORDER BY a.geolocation_zip_code_prefix;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.product_category_name_translation
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.product_category_name_translation';
        TRUNCATE TABLE silver.product_category_name_translation;
        PRINT '>> Inserting Data Into: silver.product_category_name_translation';
        INSERT INTO silver.product_category_name_translation (
            product_category_name,
            product_category_name_english
        )
        SELECT
            TRIM(column1) AS product_category_name,
            TRIM(column2) AS product_category_name_english
        FROM bronze.product_category_name_translation
        WHERE NOT (column1 = 'product_category_name'
                   AND column2 = 'product_category_name_english')
        ORDER BY column1;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_products_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_products_dataset';
        TRUNCATE TABLE silver.olist_products_dataset;
        PRINT '>> Inserting Data Into: silver.olist_products_dataset';
        INSERT INTO silver.olist_products_dataset (
            product_id,
            product_category_name,
            product_name_lenght,
            product_description_lenght,
            product_photos_qty,
            product_weight_g,
            product_length_cm,
            product_height_cm,
            product_width_cm
        )
        SELECT
            product_id,
            product_category_name,
            product_name_lenght,
            CASE 
                WHEN product_description_lenght IS NULL THEN 0
                ELSE product_description_lenght
            END AS product_description_lenght,

            CASE 
                WHEN product_photos_qty IS NULL THEN 0
                ELSE product_photos_qty
            END AS product_photos_qty,

            CASE
                WHEN product_weight_g = 0 THEN NULL
                ELSE product_weight_g
            END AS product_weight_g,

            CASE
                WHEN product_length_cm = 0 THEN NULL
                ELSE product_length_cm
            END AS product_length_cm,

            CASE
                WHEN product_height_cm = 0 THEN NULL
                ELSE product_height_cm
            END AS product_height_cm,

            CASE
                WHEN product_width_cm = 0 THEN NULL
                ELSE product_width_cm
            END AS product_width_cm
        FROM bronze.olist_products_dataset;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_sellers_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_sellers_dataset';
        TRUNCATE TABLE silver.olist_sellers_dataset;
        PRINT '>> Inserting Data Into: silver.olist_sellers_dataset';
        WITH CleanedSellers AS (
            SELECT
                seller_id,
                seller_zip_code_prefix,
                CASE        
                        WHEN LOWER(TRIM(seller_city)) LIKE '%santo andre%' THEN 'Santo André'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%sao paulo%' THEN 'são paulo' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%maua%' THEN 'mauá' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%mogi das cruzes%' THEN 'mogi das cruzes' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%rio de janeiro%' THEN 'rio de janeiro' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%barbacena%' THEN 'barbacena' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%andira%' THEN 'andirá' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%pinhais%' THEN 'pinhais' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%santa barbara d%oeste%' THEN 'santa barbara' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%ribeirao preto%' THEN 'ribeirao preto' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%carapicuiba%' THEN 'carapicuiba' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%sao sebastiao da grama%' THEN 'sao sebastiao da grama' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%jacarei%' THEN 'jacarei'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%Sp / sp' THEN 'são paulo'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%porto seguro%' THEN 'porto seguro'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%Novo hamburgo%'THEN 'novo hamburgo'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%Cariacica%' THEN 'cariacica'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%Sa~o paulo%' THEN 'são paulo' 
                        WHEN LOWER(TRIM(seller_city)) LIKE '%Sbc/sp%' THEN 'são paulo'
                        WHEN LOWER(TRIM(seller_city)) LIKE '%vendas@%' THEN NULL
                        ELSE
                            UPPER(LEFT(seller_city,1)) + LOWER(SUBSTRING(seller_city,2, LEN(seller_city)))
                    END AS clean_city,
            UPPER(TRIM(seller_state)) AS clean_state
            FROM bronze.olist_sellers_dataset
        ),
        CityStateFreq AS (
            SELECT
                seller_zip_code_prefix,
                clean_city,
                clean_state,
                COUNT(*) AS freq
            FROM CleanedSellers
            GROUP BY seller_zip_code_prefix, clean_city, clean_state
        ),
        MostFreqCityState AS (
            SELECT DISTINCT
                seller_zip_code_prefix,
                FIRST_VALUE(clean_city) OVER(PARTITION BY seller_zip_code_prefix ORDER BY freq DESC) AS city,
                FIRST_VALUE(clean_state) OVER(PARTITION BY seller_zip_code_prefix ORDER BY freq DESC) AS state
            FROM CityStateFreq
        ),
        SellerFinal AS (
            SELECT
                s.seller_id,
                s.seller_zip_code_prefix,
                m.city AS seller_city,
                m.state AS seller_state,
                ROW_NUMBER() OVER(PARTITION BY s.seller_id ORDER BY s.seller_id) AS rn
            FROM bronze.olist_sellers_dataset s
            LEFT JOIN MostFreqCityState m
                ON s.seller_zip_code_prefix = m.seller_zip_code_prefix
        )
        INSERT INTO silver.olist_sellers_dataset (
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        )
        SELECT
            seller_id,
            seller_zip_code_prefix,
            seller_city,
            seller_state
        FROM SellerFinal
        WHERE rn = 1
        ORDER BY seller_zip_code_prefix, seller_id;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_customers_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_customers_dataset';
        TRUNCATE TABLE silver.olist_customers_dataset;
        PRINT '>> Inserting Data Into: silver.olist_customers_dataset';
        WITH NormalizeCity AS (
        SELECT
            customer_id,
            customer_unique_id,
            CAST(customer_zip_code_prefix AS VARCHAR(5)) AS zip_code_prefix,
                    CASE        
                    WHEN LOWER(TRIM(customer_city)) LIKE '%santo andre%' THEN 'Santo André'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%sao paulo%' THEN 'São Paulo' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%maua%' THEN 'Mauá' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%mogi das cruzes%' THEN 'Mogi das Cruzes' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%rio de janeiro%' THEN 'Rio de Janeiro' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%barbacena%' THEN 'Barbacena'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%ji-parana%' THEN 'Ji-Paraná'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%andira%' THEN 'andirá' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%pinhais%' THEN 'Pinhais' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%santa barbara d%oeste%' THEN 'santa barbara' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%ribeirao preto%' THEN 'ribeirao preto' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%carapicuiba%' THEN 'carapicuiba' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%sao sebastiao da grama%' THEN 'sao sebastiao da grama' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%jacarei%' THEN 'jacarei'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%Sp / sp' THEN 'são paulo'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%porto seguro%' THEN 'porto seguro'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%Novo hamburgo%'THEN 'novo hamburgo'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%Cariacica%' THEN 'cariacica'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%Sa~o paulo%' THEN 'são paulo' 
                    WHEN LOWER(TRIM(customer_city)) LIKE '%Sbc/sp%' THEN 'são paulo'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%SAO PAULO%' THEN 'São Paulo'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%RIO DE JANEIRO%' THEN 'Rio de Janeiro'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%BELO HORIZONTE%' THEN 'Belo Horizonte'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%BRASILIA%' OR LOWER(TRIM(customer_city)) LIKE '%BRASÍLIA%' THEN 'Brasília'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%CURITIBA%' THEN 'Curitiba'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%PORTO ALEGRE%' THEN 'Porto Alegre'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%SALVADOR%' THEN 'Salvador'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%RECIFE%' THEN 'Recife'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%FORTALEZA%' THEN 'Fortaleza'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%(%)%' THEN LEFT(customer_city, CHARINDEX('(', customer_city + '(') - 1)
                    -- Remove d'oeste / d oeste / -oeste / d'aliança / d'aguá etc.
                    WHEN LOWER(TRIM(customer_city)) LIKE '% D''%' OR LOWER(TRIM(customer_city)) LIKE '% D %' OR LOWER(TRIM(customer_city)) LIKE '%- %'
                        THEN TRIM(LEFT(customer_city, CHARINDEX(' D', customer_city + ' D') - 1))

                    -- Remove trailing -acu / -mirim / -guaçu etc.
                    WHEN LOWER(TRIM(customer_city)) LIKE '%-%' 
                        THEN TRIM(LEFT(customer_city, CHARINDEX('-', customer_city + '-') - 1))
                    WHEN LOWER(TRIM(customer_city)) LIKE '%QUILOMETRO%' 
                        OR LOWER(TRIM(customer_city)) LIKE '%QUILOMETRO 14%' 
                        OR LOWER(TRIM(customer_city)) = 'Quilometro 14 do mutum' 
                            THEN 'Porto Seguro'
                    WHEN LOWER(TRIM(customer_city)) LIKE '%vendas@%' THEN NULL
                        ELSE
                            UPPER(LEFT(customer_city,1)) + LOWER(SUBSTRING(customer_city,2, LEN(customer_city)))
                    END AS clean_city,   
                UPPER(TRIM(customer_state)) AS clean_state
            FROM bronze.olist_customers_dataset
            ),
            CityStateFreq AS (
                SELECT
                    customer_unique_id,
                    zip_code_prefix,
                    clean_city,
                    clean_state,
                    COUNT(*) AS freq
                FROM NormalizeCity
                GROUP BY customer_unique_id, zip_code_prefix, clean_city, clean_state
            ),
            MostFreqMapping AS (
                SELECT DISTINCT
                    customer_unique_id,
                    FIRST_VALUE(zip_code_prefix) OVER(PARTITION BY customer_unique_id ORDER BY freq DESC) AS zip_code_prefix,
                    FIRST_VALUE(clean_city) OVER(PARTITION BY customer_unique_id ORDER BY freq DESC) AS customer_city,
                    FIRST_VALUE(clean_state) OVER(PARTITION BY customer_unique_id ORDER BY freq DESC) AS customer_state
                FROM CityStateFreq
            ),
            CustomerFinal AS (
                SELECT
                    n.customer_id,
                    m.customer_unique_id,
                    m.zip_code_prefix AS customer_zip_code_prefix,
                    m.customer_city,
                    m.customer_state
                FROM NormalizeCity n
                LEFT JOIN MostFreqMapping m
                    ON n.customer_unique_id = m.customer_unique_id
            )
            INSERT INTO silver.olist_customers_dataset (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
            )
            SELECT
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state
        FROM CustomerFinal
        ORDER BY customer_unique_id;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_orders_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_orders_dataset';
        TRUNCATE TABLE silver.olist_orders_dataset;
        PRINT '>> Inserting Data Into: silver.olist_orders_dataset';
        WITH purchase_approval AS (
            SELECT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
        
                CASE 
                    WHEN order_approved_at < order_purchase_timestamp
                        THEN order_purchase_timestamp
                    ELSE order_approved_at
                END AS order_approved_at,

                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date
            FROM bronze.olist_orders_dataset
        ),
        approval_carrier AS (
            SELECT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,

                CASE 
                    WHEN order_delivered_carrier_date < order_approved_at
                        THEN order_approved_at
                    ELSE order_delivered_carrier_date
                END AS order_delivered_carrier_date,

                order_delivered_customer_date,
                order_estimated_delivery_date
            FROM purchase_approval
        ),
        carrier_customer AS (
            SELECT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,

                CASE
                    WHEN order_delivered_customer_date < order_delivered_carrier_date
                        THEN order_delivered_carrier_date
                    ELSE order_delivered_customer_date
                END AS order_delivered_customer_date,

                order_estimated_delivery_date
            FROM approval_carrier
        ),
        avg_delivery_time AS (
            SELECT 
                c.customer_unique_id,
                AVG(DATEDIFF(day, o.order_approved_at, o.order_delivered_customer_date)) AS avg_delivery_days
            FROM silver.olist_orders_dataset o
            LEFT JOIN silver.olist_customers_dataset c
                ON o.customer_id = c.customer_id
            WHERE o.order_delivered_customer_date IS NOT NULL
              AND o.order_approved_at IS NOT NULL
            GROUP BY c.customer_unique_id
        )
        INSERT INTO silver.olist_orders_dataset (
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date
        )
        SELECT
            f.order_id,
            f.customer_id,
            f.order_status,
            f.order_purchase_timestamp,
            f.order_approved_at,
            f.order_delivered_carrier_date,
            f.order_delivered_customer_date,

            CASE
                WHEN f.order_estimated_delivery_date < f.order_approved_at
                    THEN DATEADD(
                            DAY,
                            COALESCE(a.avg_delivery_days, 10),
                            f.order_approved_at
                         )
                ELSE f.order_estimated_delivery_date
            END
        FROM carrier_customer f
        LEFT JOIN silver.olist_customers_dataset c
            ON f.customer_id = c.customer_id
        LEFT JOIN avg_delivery_time a
            ON c.customer_unique_id = a.customer_unique_id;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.dim_date
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.dim_date';
        TRUNCATE TABLE silver.dim_date;
        PRINT '>> Inserting Data Into: silver.dim_date';
        ;WITH DateSequence AS (
                SELECT CAST('2016-01-01' AS DATE) AS d
                UNION ALL
                SELECT DATEADD(DAY, 1, d)
                FROM DateSequence
                WHERE d < '2020-01-01'
            ),
            BrazilHolidays AS (
                SELECT 
                    d,
                    CASE 
                        WHEN d = DATEFROMPARTS(YEAR(d),1,1) THEN 'New Year'
                        WHEN MONTH(d)=2 AND DAY(d)=DATEPART(DAY, DATEADD(DAY,-46, DATEFROMPARTS(YEAR(d),4,1))) THEN 'Carnaval Monday'
                        WHEN MONTH(d)=2 AND DAY(d)=DATEPART(DAY, DATEADD(DAY,-47, DATEFROMPARTS(YEAR(d),4,1))) THEN 'Carnaval Tuesday'
                        WHEN d = DATEADD(DAY, -2, DATEFROMPARTS(YEAR(d),4,1)) THEN 'Good Friday'
                        WHEN d = DATEFROMPARTS(YEAR(d),4,21) THEN 'Tiradentes'
                        WHEN d = DATEFROMPARTS(YEAR(d),5,1) THEN 'Work Day'
                        WHEN d = DATEFROMPARTS(YEAR(d),9,7) THEN 'Independence Day'
                        WHEN d = DATEFROMPARTS(YEAR(d),10,12) THEN 'Children’s Day'
                        WHEN d = DATEFROMPARTS(YEAR(d),11,2) THEN 'Finados'
                        WHEN d = DATEFROMPARTS(YEAR(d),11,15) THEN 'Proclamation Day'
                        WHEN d = DATEFROMPARTS(YEAR(d),12,25) THEN 'Christmas'
                        ELSE NULL 
                    END AS holiday_name
                FROM DateSequence
            ),

            SpecialRetail AS (
                SELECT 
                    ds.d,
                    -- Black Friday = 4th Friday of November
                    CAST(
                        CASE 
                            WHEN ds.d = DATEADD(WEEK, 3,
                                                    DATEADD(DAY, (6 - DATEPART(WEEKDAY, DATEFROMPARTS(YEAR(ds.d),11,1)) + 7) % 7,
                                                            DATEFROMPARTS(YEAR(ds.d),11,1)))
                            THEN 1 ELSE 0 
                        END AS BIT
                    ) AS is_black_friday,
                    -- Mother's Day = 2nd Sunday of May
                    CAST(
                        CASE 
                            WHEN DATEPART(WEEKDAY, DATEFROMPARTS(YEAR(ds.d),5,1)) = 1 
                                    THEN CASE WHEN ds.d = DATEADD(DAY, 7, DATEFROMPARTS(YEAR(ds.d),5,1)) THEN 1 ELSE 0 END
                                    ELSE CASE WHEN ds.d = DATEADD(DAY, (14 - DATEPART(WEEKDAY, DATEFROMPARTS(YEAR(ds.d),5,1))) % 7 + 7, DATEFROMPARTS(YEAR(ds.d),5,1)) THEN 1 ELSE 0 END
                        END AS BIT
                    ) AS is_mothers_day,
                    -- Fixed dates for other retail days
                    CAST(CASE WHEN MONTH(ds.d)=6 AND DAY(ds.d)=12 THEN 1 ELSE 0 END AS BIT) AS is_valentines_day,
                    CAST(CASE WHEN MONTH(ds.d)=10 AND DAY(ds.d)=12 THEN 1 ELSE 0 END AS BIT) AS is_childrens_day,
                    CAST(CASE WHEN MONTH(ds.d)=3 AND DAY(ds.d)=15 THEN 1 ELSE 0 END AS BIT) AS is_consumers_day
                FROM DateSequence ds
            )

            INSERT INTO silver.dim_date (
                date_key,
                full_date,
                year,
                year_text,
                quarter_number,
                quarter,
                month_number,
                month_text,
                month_name_full,
                month_name_short,
                week_number_iso,
                week_text,
                day_of_month,
                day_of_year,
                day_name_full,
                day_name_short,
                is_weekend,
                is_weekday,
                is_brazilian_holiday,
                holiday_name,
                is_black_friday,
                is_mothers_day,
                is_valentines_day,
                is_childrens_day,
                is_consumers_day,
                fiscal_year,
                fiscal_quarter
            )
            SELECT 
                CAST(FORMAT(ds.d,'yyyyMMdd') AS INT) AS date_key,
                ds.d AS full_date,
                YEAR(ds.d) AS year,
                FORMAT(ds.d,'yyyy') AS year_text,
                DATEPART(QUARTER, ds.d) AS quarter_number,
                'Q' + CAST(DATEPART(QUARTER, ds.d) AS CHAR(1)) AS quarter,
                MONTH(ds.d) AS month_number,
                FORMAT(ds.d,'MM') AS month_text,
                FORMAT(ds.d,'MMMM') AS month_name_full,
                FORMAT(ds.d,'MMM') AS month_name_short,
                DATEPART(ISO_WEEK, ds.d) AS week_number_iso,
                'W' + FORMAT(DATEPART(ISO_WEEK, ds.d),'00') AS week_text,
                DAY(ds.d) AS day_of_month,
                DATEPART(DAYOFYEAR, ds.d) AS day_of_year,
                DATENAME(WEEKDAY, ds.d) AS day_name_full,
                LEFT(DATENAME(WEEKDAY, ds.d),3) AS day_name_short,
                CAST(CASE WHEN DATENAME(WEEKDAY, ds.d) IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS BIT) AS is_weekend,
                CAST(CASE WHEN DATENAME(WEEKDAY, ds.d) NOT IN ('Saturday','Sunday') THEN 1 ELSE 0 END AS BIT) AS is_weekday,
                CAST(CASE WHEN h.holiday_name IS NOT NULL THEN 1 ELSE 0 END AS BIT) AS is_brazilian_holiday,
                h.holiday_name,
                s.is_black_friday,
                s.is_mothers_day,
                s.is_valentines_day,
                s.is_childrens_day,
                s.is_consumers_day,
                CASE WHEN MONTH(ds.d) >= 7 THEN YEAR(ds.d) + 1 ELSE YEAR(ds.d) END AS fiscal_year,
                CASE
                    WHEN MONTH(ds.d) BETWEEN 7 AND 9 THEN 'FQ1'
                    WHEN MONTH(ds.d) BETWEEN 10 AND 12 THEN 'FQ2'
                    WHEN MONTH(ds.d) BETWEEN 1 AND 3 THEN 'FQ3'
                    WHEN MONTH(ds.d) BETWEEN 4 AND 6 THEN 'FQ4'
                END AS fiscal_quarter
            FROM DateSequence ds
            LEFT JOIN BrazilHolidays h ON ds.d = h.d
            LEFT JOIN SpecialRetail s ON ds.d = s.d
            OPTION (MAXRECURSION 0);
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_order_payments_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_order_payments_dataset';
        TRUNCATE TABLE silver.olist_order_payments_dataset;
        PRINT '>> Inserting Data Into: silver.olist_order_payments_dataset';
        INSERT INTO silver.olist_order_payments_dataset(
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value
        )
        SELECT
            order_id,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY payment_sequential) AS payment_sequential,
            payment_type,
            CASE 
                WHEN payment_type = 'credit_card' AND payment_installments = 0 THEN 1
                ELSE payment_installments
            END AS payment_installments,
            payment_value
        FROM bronze.olist_order_payments_dataset;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_order_reviews_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_order_reviews_dataset';
        TRUNCATE TABLE silver.olist_order_reviews_dataset;
        PRINT '>> Inserting Data Into: silver.olist_order_reviews_dataset';
        WITH Deduplicated AS (
            SELECT *,
                   ROW_NUMBER() OVER (PARTITION BY review_id ORDER BY review_creation_date DESC) AS rn
            FROM bronze.olist_order_reviews_dataset
        )
        , CleanedText AS (
            SELECT
                review_id,
                order_id,
                review_score,
                NULLIF(LTRIM(RTRIM(review_comment_title)),'') AS review_comment_title,
                NULLIF(LTRIM(RTRIM(review_comment_message)),'') AS review_comment_message,
                review_creation_date,
                review_answer_timestamp
            FROM Deduplicated
            WHERE rn = 1
        )
        INSERT INTO silver.olist_order_reviews_dataset (
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        )
        SELECT
            review_id,
            order_id,
            review_score,
            review_comment_title,
            review_comment_message,
            review_creation_date,
            review_answer_timestamp
        FROM CleanedText;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';


        -- Loading silver.olist_order_items_dataset
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: silver.olist_order_items_dataset';
        TRUNCATE TABLE silver.olist_order_items_dataset;
        PRINT '>> Inserting Data Into: silver.olist_order_items_dataset';
        INSERT INTO silver.olist_order_items_dataset (
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        )
        SELECT
            order_id,
            ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY order_item_id) AS order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value
        FROM bronze.olist_order_items_dataset;
        SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';
        
        SET @batch_end_time = GETDATE();
		PRINT '=========================================='
		PRINT 'Loading Silver Layer is Completed';
        PRINT '   - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '=========================================='
		
    END TRY
	BEGIN CATCH
		PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST (ERROR_STATE() AS NVARCHAR);
		PRINT '=========================================='
	END CATCH
END



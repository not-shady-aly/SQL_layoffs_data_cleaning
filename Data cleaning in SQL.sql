SELECT *
FROM layoffs;

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT *
FROM layoffs_staging;


INSERT layoffs_staging
SELECT *
FROM layoffs;
# 1. remove the duplicates
WITH remove_dub_cte AS 
(SELECT * , ROW_NUMBER() OVER(
PARTITION BY company , location , industry ,total_laid_off, 
percentage_laid_off , `date` , stage , country , funds_raised_millions) AS detector
FROM layoffs_clean)
SELECT *
FROM remove_dub_cte
WHERE detector > 1;

CREATE TABLE `layoffs_clean` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

INSERT INTO layoffs_clean
SELECT * , ROW_NUMBER() OVER(
PARTITION BY company , location , industry ,total_laid_off, 
percentage_laid_off , `date` , stage , country , funds_raised_millions) AS detector
FROM layoffs_staging;

DELETE
FROM layoffs_clean
WHERE row_num > 1;

SELECT *
FROM layoffs_clean;

# 2. consistancy
UPDATE layoffs_clean
SET company = TRIM(company);

SELECT DISTINCT country
FROM layoffs_clean
ORDER BY 1;

SELECT `date`
FROM layoffs_clean;

UPDATE layoffs_clean
SET country = 'United States'
WHERE country = 'United States.';

UPDATE layoffs_clean
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%' ;

SELECT *
FROM layoffs_clean;

UPDATE layoffs_clean
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER TABLE layoffs_clean
MODIFY COLUMN `date` DATE;

# 3. nulls or blanks
UPDATE layoffs_clean
SET industry = null
WHERE industry = '';

SELECT *
FROM layoffs_clean t1
JOIN layoffs_clean t2
	ON t1.company = t2.company
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;

UPDATE layoffs_clean t1
JOIN layoffs_clean t2
	ON t1.company = t2.company
SET t2.industry = t1.industry
WHERE t1.industry IS NULL AND t2.industry IS NOT NULL;
# 4. remove unimportant coulumns

SELECT *
FROM layoffs_clean
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_clean
WHERE total_laid_off IS NULL AND percentage_laid_off IS NULL
;
ALTER TABLE layoffs_clean
DROP COLUMN row_num;

SELECT *
FROM layoffs_clean;
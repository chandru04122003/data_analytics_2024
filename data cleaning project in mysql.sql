select * 
from layoffs;

create table layoffs_staging
like layoffs;

select * 
from layoffs_staging;

insert layoffs_staging
select *
from layoffs;

select *,
ROW_NUMBER() OVER (
PARTITION BY company, industry, total_laid_off,percentage_laid_off,`date`) AS row_num
     from layoffs_staging;   

with duplicate_cte as
(
select *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num     
from layoffs_staging
)
select *
from duplicate_cte
where row_num>1;
     
select * 
from layoffs_staging
where company='casper';

with duplicate_cte as
(
select *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num     
from layoffs_staging
)
delete
from duplicate_cte
where row_num>1;

CREATE TABLE `layoffs_staging2` (
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

select * 
from layoffs_staging2
where row_num >1;

insert into layoffs_staging2
select *,
ROW_NUMBER() OVER (
PARTITION BY company, location, industry, total_laid_off,percentage_laid_off,`date`, stage, country, funds_raised_millions) AS row_num     
from layoffs_staging;

delete
from layoffs_staging2
where row_num >1;

select *
from layoffs_staging2;

-- standardizing data

select company,trim(company)
from layoffs_staging2;

update layoffs_staging2
set company=trim(company);

select distinct industry
from layoffs_staging2;

update layoffs_staging2
set industry='Crypto'
where industry like 'Crypto%';

select distinct country, trim(trailing '.' from country)
from layoffs_staging2
order by 1;

update layoffs_staging2
set country=trim(trailing '.' from country) 
where country like 'united states%';

select *
from layoffs_staging2;

alter table layoffs_staging2
modify column  `date` date;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

update layoffs_staging2
set  industry = null
where industry='';


select *
from layoffs_staging2
where industry is null
or industry='';

select *
from layoffs_staging2
where company like 'bally%';

select t1.industry, t2.industry
from layoffs_staging2 t1
JOIN layoffs_staging2 t2
   ON t1.company = t2.company
where (t1.industry IS NULL or t1.industry='')
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
    ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


select *
from layoffs_staging2;

select *
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;


select *
from layoffs_staging2;

alter table layoffs_staging2
drop column row_num;






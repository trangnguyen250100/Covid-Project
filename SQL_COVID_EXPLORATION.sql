-- THIS IS THE COVID DATASET

set sql_safe_updates = 0; 

-- IMPORT DATA FROM CSV FILES

create table covid_vaccination 
( 
continent char(255), 
location char(255), 
date text,
total_vaccinations text, 
new_vaccinations text, 
population text
); 

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/covid_vaccination.csv"
into table covid_vaccination
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows; 

create table covid_death
( 
continent char(255), 
location char(255), 
date text,
total_cases text, 
new_cases text, 
total_deaths text, 
new_deaths text,
population text
); 

load data infile "C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/covid_death.csv"
into table covid_death
fields terminated by ','
enclosed by '"'
lines terminated by '\n'
ignore 1 rows; 

-----------------------------------------------------------------------------------------------------

-- STANDARDIZE FORMAT

-- Covid deaths table 

select date, cast(date as date) 
from covid_death; 

update covid_death
set date = cast(date as date); 

-- If it doesn't update properly, add converted columns

alter table covid_death
add date_converted date, 

update covid_death
set date_converted = str_to_date(date, '%d %m %Y'),
total_cases = cast(total_cases as signed),
new_cases = cast(new_cases as signed),
total_deaths = cast(total_deaths as signed),
new_deaths = cast(new_deaths as signed),
population = cast(population as signed); 

select * from covid_death; 

-- Covid vaccinations table 

alter table covid_vaccination
add date_converted date,

update covid_vaccination
set date_converted = str_to_date(date, '%d %m %Y'),
total_vaccinations = cast(total_vaccinations as signed),
new_vaccinations = cast(new_vaccinations as signed),
population = cast(population as signed); 

select * 
from covid_vaccination; 

---------------------------------------------------------------------------------------------------

-- DELETE RECORDS WITHOUT VALUE IN CONTINENT COLUMN

-- Covid deaths table 

select *
from covid_death
where continent = ''; 

delete 
from covid_death
where continent = ''; 

-- Covid vaccinations table 

select *
from covid_vaccination
where continent = ''; 

delete 
from covid_vaccination
where continent = '';  


-----------------------------------------------------------------------------------------------------

-- CREATE INDEX COLUMNS

create index continent on covid_death (continent);
create index name on covid_death (location, date_converted); 

create index continent on covid_vaccination (continent);
create index name on covid_vaccination (location, date_converted); 

-----------------------------------------------------------------------------------------------------

-- EXPLORATION 

-- Globar numbers 

select sum(new_cases) as total_cases, sum(new_deaths) as total_deaths, 
(sum(new_deaths)/sum(new_cases))*100 as death_percentage 
from covid_death; 

-- Total death count per continent 

select continent, sum(new_deaths) as total_death_count 
from covid_death
group by continent
order by total_death_count desc; 

-- Total death count per country

select location, sum(new_deaths) as total_death_count
from covid_death
group by location
order by total_death_count desc; 

-- Show the probability of dying if infected with covid-19 in each country 

select location, date_converted, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_percentage
from covid_death
order by 1,2; 

-- Show the probability of dying if infected with covid-19 in Vietnam 

select location, date_converted, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_percentage
from covid_death
where location = 'Vietnam'
order by 2; 

-- Show percentage of population infected and died by covid-19

select location, date_converted, population, total_cases, 
	(total_cases/population)*100 as percent_population_infected,
    (total_deaths/population)*100 as percent_population_died
from covid_death
order by 1,2; 

-- Countries with infection rate compared to population 

select location, population, max(total_cases) as total_infection_count, 
	(max(total_cases)/population)*100 as percent_population_infected
from covid_death
group by location, population
order by percent_population_infected desc; 

select location, population, date_converted, max(total_cases) as total_infection_count, 
	(max(total_cases)/population)*100 as percent_population_infected
from covid_death
group by location, population, date_converted
order by percent_population_infected desc; 

-- Percentage of population in each country that has received at least one covid vaccine 

select d.continent, d.location, d.date_converted, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date_converted = v.date_converted
order by d.location, d.date_converted; 

-- Using CTE to perform calculation on partition by in previous query 
with PopulationvsVaccinations (continent, location, date, population, new_vaccinations, rolling_people_vaccinated) 
as
(
select d.continent, d.location, d.date_converted, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date_converted = v.date_converted
)
select *, (people_vaccinated/population)*100
from PopulationvsVaccinations; 

-- Using Temp table to perform calculation on partition by in previous query 

drop table if exists percent_population_vaccinated;

create temporary table percent_population_vaccinated
(
continent char(255), 
location char(255), 
date date, 
population int, 
new_vaccinations int, 
people_vaccinated int,
index (continent, location, date)
); 

insert into percent_population_vaccinated
select d.continent, d.location, d.date_converted, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date_converted = v.date_converted
order by d.location, d.date_converted; 

select *, (people_vaccinated/population)*100
from percent_population_vaccinated; 

-- Create a view to store a new data table and perform calculations

create view PercentPopulationVaccinated 
as 
select d.continent, d.location, d.date_converted, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date_converted = v.date_converted; 

select *, (people_vaccinated/population)*100
from PercentPopulationVaccinated;

-----------------------------------------------------------------------------------------------

-- EXPORT DATA USED FOR TABLEAU PROJECT 

-- Countries with infection rate compared to population 
select 'Location', 'Population', 'Total Infection Count', 'Percent Population Infected' 
union all 
select location, population, max(total_cases) as total_infection_count, 
	(max(total_cases)/population)*100 as percent_population_infected
from covid_death
group by location, population
order by percent_population_infected desc
into outfile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TABLE3.csv' 
fields terminated by ','; 

-- Percentage of population in each country that has received at least one covid vaccine 
select 'Continent', 'Location', 'Date', 'Population', 'People Vaccinated', 'Percent People Vaccinated'
union all
select continent, location, date, population, people_vaccinated, (people_vaccinated/population)*100
from PercentPopulationVaccinated
into outfile 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/TABLE4.csv' 
fields terminated by ','; 


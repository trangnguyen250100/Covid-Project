-- THIS IS THE COVID DATASET UNTIL 2021/11/23

set sql_safe_updates = 0; 

-- IMPORT DATA FROM CSV FILES

create table covid_vaccination 
( 
continent text, 
location text, 
date text,
total_vaccinations text(11), 
new_vaccinations text(11), 
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
continent text, 
location text, 
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
add continent_converted char(255), 
add location_converted char(255), 
add date_converted date, 
add total_cases_converted int, 
add new_cases_converted int,
add total_deaths_converted int, 
add new_deaths_converted int, 
add population_converted int;

update covid_death
set continent_converted = cast(continent as char(255));  

update covid_death
set location_converted = cast(location as char(255)); 

update covid_death
set date_converted = str_to_date(date, '%d %m %Y'); 

update ignore covid_death
set total_cases_converted = cast(total_cases as signed); 

update ignore covid_death
set new_cases_converted = cast(new_cases as signed); 

update ignore covid_death
set total_deaths_converted = cast(total_deaths as signed); 

update ignore covid_death
set new_deaths_converted = cast(new_deaths as signed); 

update ignore covid_death 
set population_converted = cast(population as signed); 

select * from covid_death; 

-- Covid vaccinations table 

alter table covid_vaccination
add continent_converted char(255), 
add location_converted char(255), 
add date_converted date,
add total_vaccinations_converted int,
add new_vaccinations_converted int,
add population_converted int; 

update covid_vaccination
set continent_converted = cast(continent as char(255));

update covid_vaccination
set location_converted = cast(location as char(255)); 

update covid_vaccination
set date_converted = str_to_date(date, '%d %m %Y'); 

update ignore covid_vaccination
set total_vaccinations_converted = cast(total_vaccinations as signed); 

update ignore covid_vaccination
set new_vaccinations_converted = cast(new_vaccinations as signed);

update ignore covid_vaccination
set population_converted = cast(population as signed); 

select * 
from covid_vaccination; 

---------------------------------------------------------------------------------------------------

-- DELETE UNUSED COLUMNS & RENAME THE CONVERTED COLUMNS

-- Covid deaths table 

alter table covid_death
drop column continent;

alter table covid_death
drop column location; 

alter table covid_death
drop column date; 

alter table covid_death
drop column total_cases; 

alter table covid_death
drop column new_cases; 

alter table covid_death
drop column total_deaths; 

alter table covid_death
drop column new_deaths; 

alter table covid_death
drop column population; 


alter table covid_death
change `continent_converted` `continent` char(255),
change `location_converted` `location` char(255), 
change `date_converted` `date` date,
change `total_cases_converted` `total_cases` int,
change `new_cases_converted` `new_cases` int, 
change `total_deaths_converted` `total_deaths` int, 
change `new_deaths_converted` `new_deaths` int,
change `population_converted` `population` int; 

select * 
from covid_death; 

-- Covid vaccinations table 

alter table covid_vaccination
drop column continent; 

alter table covid_vaccination
drop column location; 

alter table covid_vaccination
drop column date; 

alter table covid_vaccination
drop column total_vaccinations; 

alter table covid_vaccination
drop column new_vaccinations; 

alter table covid_vaccination
drop column population;

alter table covid_vaccination
change `continent_converted` `continent` char(255), 
change `location_converted` `location` char(255),
change `date_converted` `date` date, 
change `total_vaccinations_converted` `total_vaccinations` int, 
change `new_vaccinations_converted` `new_vaccinations` int,
change `population_converted` `population` int; 

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
create index location on covid_death (location); 
create index date on covid_death (date); 

create index continent on covid_vaccination (continent);
create index location on covid_vaccination (location); 
create index date on covid_vaccination (date); 


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

select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_percentage
from covid_death
order by 1,2; 

-- Show the probability of dying if infected with covid-19 in Vietnam 

select location, date, total_cases, total_deaths, (total_deaths/total_cases)*100 as death_percentage
from covid_death
where location = 'Vietnam'
order by 2; 

-- Show percentage of population infected and died by covid-19 

select location, date, population, total_cases, 
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

select location, population, date, max(total_cases) as total_infection_count, 
	(max(total_cases)/population)*100 as percent_population_infected
from covid_death
group by location, population, date
order by percent_population_infected desc; 

-- Percentage of population in each country that has received at least one covid vaccine 

select d.continent, d.location, d.date, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date = v.date
order by d.location, d.date; 

-- Using CTE to perform calculation on partition by in previous query 
with PopulationvsVaccinations (continent, location, date, population, new_vaccinations, rolling_people_vaccinated) 
as
(
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date = v.date
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
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date = v.date
order by d.location, d.date; 

select *, (people_vaccinated/population)*100
from percent_population_vaccinated; 

-- Create a view to store a new data table and perform calculations

create view PercentPopulationVaccinated 
as 
select d.continent, d.location, d.date, d.population, v.new_vaccinations,
	sum(v.new_vaccinations) over (partition by d.location) as people_vaccinated
from covid_death d 
join covid_vaccination v 
	on d.location = v.location
    and d.date = v.date; 

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


create or replace table w31(id int, hero_name string, villains_defeated number);

insert into w31 values
  (1, 'Pigman', 5),
  (2, 'The OX', 10),
  (3, 'Zaranine', 4),
  (4, 'Frostus', 8),
  (5, 'Fridayus', 1),
  (6, 'SheFrost', 13),
  (7, 'Dezzin', 2.3),
  (8, 'Orn', 7),   
  (9, 'Killder', 6),   
  (10, 'PolarBeast', 11)
  ;
  
  SELECT * FROM W31;
  
SELECT 
  MAX_BY (hero_name, villains_defeated) as BEST_HERO,
  MIN_BY (hero_name, villains_defeated) as WORST_HERO
FROM W31;

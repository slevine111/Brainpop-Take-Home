create TABLE monthly_login_aggregates(
    user_type varchar NOT NULL,
    user_id int NOT NULL,
    year int NOT NULL,
    month varchar NOT NULL,
    num_logins int NOT NULL,
    device varchar NOT NULL,
    d_last_accessed bigint NOT NULL,
    PRIMARY KEY(user_id, year, month, device));


create TABLE daily_login_aggregates(
    date date NOT NULL,
    user_type varchar NOT NULL,
    user_id int NOT NULL,
    num_logins int NOT NULL,
    device varchar NOT NULL,
    PRIMARY KEY(date, user_id, device));

insert into monthly_login_aggregates values
('student',20259112,2020,'06',201,'iPad',1592798091),
('student',25676560,2020,'06',89,'Chrome',1592770100),
('teacher',8413059,2020,'06',45,'Safari',1592796082),
('student',15681182,2020,'06',28,'iPad',1592773886),
('student',25588005,2020,'06',94,'mobile',1592793399),
('teacher',10074491,2020,'06',23,'Chrome',1592769841),
('student',20838865,2020,'06',26,'Firefox',1592747794),
('teacher',8114154,2020,'06',104,'Chrome',1592790108),
('student',22530878,2020,'06',138,'Chrome',1592799127),
('student',19958413,2020,'06',20,'Chrome',1592794109),
('teacher',9879522,2020,'06',354,'Chrome',1592797612),
('student',22297341,2020,'06',16,'Firefox',1592797528),
('student',25478573,2020,'06',12,'Safari',1592757016),
('student',22495395,2020,'06',30,'iPad',1592772743),
('student',20352275,2020,'06',88,'iPad',1592792554),
('student',16418706,2020,'06',35,'Chrome',1592790086),
('teacher',10235302,2020,'06',8,'Chrome',1592795185),
('teacher',10234747,2020,'06',19,'Chrome',1592713136),
('student',22629654,2020,'06',16,'Chrome',1592796666),
('student',18155370,2020,'06',36,'Chrome',1592758104),
('student',835528,2020,'06',105,'Chrome',1592790439),
('student',22317160,2020,'06',23,'iPad',1592777882),
('student',22495501,2020,'06',27,'iPad',1592794653),
('student',25295484,2020,'06',3,'Safari',1592784707),
('student',1631444,2020,'06',622,'Chrome',1592787107);

insert into daily_login_aggregates values
('2020-06-01','student',20259112,34,'iPad'),
('2020-06-01','student',25676560,89,'Chrome'),
('2020-06-03','teacher',8413059,10,'Safari'),
('2020-06-03','student',15681182,10,'iPad');

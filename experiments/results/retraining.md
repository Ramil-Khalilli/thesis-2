# Experiment 3 – retraining policy

Year 2 replayed under each policy; MAE = mean absolute error of the next-trading-day revenue forecast.

| policy | scored days | model MAE (£) | baseline MAE (£) | better than baseline | model versions | replay time (s) |
|---|---|---|---|---|---|---|
| never | 304 | 9,353 | 11,370 | 17.7% | 1 | 95.2 |
| monthly | 304 | 9,328 | 11,370 | 18.0% | 7 | 189.3 |

## MAE by month (£)

| month | baseline | never retrain | monthly retrain | best |
|---|---|---|---|---|
| 2010-12 | 14,914 | 13,425 | 13,425 | never |
| 2011-01 | 11,502 | 7,761 | 8,440 | never |
| 2011-02 | 5,396 | 4,638 | 4,719 | never |
| 2011-03 | 7,561 | 6,776 | 6,927 | never |
| 2011-04 | 7,511 | 5,188 | 5,514 | never |
| 2011-05 | 10,527 | 6,750 | 6,655 | monthly |
| 2011-06 | 13,540 | 10,024 | 10,030 | never |
| 2011-07 | 9,626 | 8,526 | 8,474 | monthly |
| 2011-08 | 8,141 | 12,116 | 10,936 | baseline |
| 2011-09 | 17,513 | 11,938 | 12,406 | never |
| 2011-10 | 15,038 | 10,717 | 10,310 | monthly |
| 2011-11 | 13,360 | 12,251 | 12,124 | monthly |
| 2011-12 | 15,928 | 14,602 | 14,568 | monthly |

## Monthly policy: retraining decisions

| as_of | event | champion | challenger_mae | champion_mae | message |
|---|---|---|---|---|---|
| 2010-11-30 | initial_model | 1 | 14833 | None | trained on 271 days |
| 2011-01-04 | promoted | 2 | 14887 | 14911 | challenger better by 24 MAE |
| 2011-02-01 | kept_champion | 2 | 7885 | 7885 | challenger worse by 0 MAE |
| 2011-03-01 | promoted | 3 | 4703 | 4708 | challenger better by 5 MAE |
| 2011-04-01 | kept_champion | 3 | 6882 | 6877 | challenger worse by 5 MAE |
| 2011-05-01 | kept_champion | 3 | 5533 | 5499 | challenger worse by 34 MAE |
| 2011-06-01 | kept_champion | 3 | 6989 | 6969 | challenger worse by 19 MAE |
| 2011-07-01 | promoted | 4 | 10506 | 10569 | challenger better by 63 MAE |
| 2011-08-01 | kept_champion | 4 | 8248 | 8201 | challenger worse by 47 MAE |
| 2011-09-01 | promoted | 5 | 10227 | 10319 | challenger better by 93 MAE |
| 2011-10-02 | promoted | 6 | 12937 | 13108 | challenger better by 171 MAE |
| 2011-11-01 | kept_champion | 6 | 10193 | 10170 | challenger worse by 23 MAE |
| 2011-12-01 | promoted | 7 | 11399 | 11500 | challenger better by 102 MAE |

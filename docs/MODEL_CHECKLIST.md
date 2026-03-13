# Model Checklist

Before creating any table or model, answer these three questions.

## 1. What is the grain?
One row represents:

## 2. What is the primary key?
Unique key(s):

## 3. What question does this table answer?
Purpose:

---

## Layer placement

Is this model:

- staging
- intermediate
- fact
- dimension

---

## Notes

### Facts
- numeric measurements
- event or transaction driven
- grain must be explicit

### Dimensions
- descriptive attributes
- one row per entity or per entity-version
- consider SCD implications

### Guardrails
- do not mix grains
- do not aggregate too early
- do not hide business logic in notebooks or BI tools
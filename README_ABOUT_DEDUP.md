
## Preproccessing
1. pauthor, ptitle inversed issue ???
2. lowercase
3. accents
4. year
5. different journalid  can be the same journal or not???
6. special characters

## Train Blocking rules (Predicates)
- produce predicates


## Apply predicates to input data (on spark)
- produce pairs


## Compute similarity metrics to pairs (on spark)
- normalised affine gap distance
- conditional random field distance
- token set ratio
- token sort ratio
- others ?????

## Scoring with logistic regression (spark ML??)


## Clustering (local)
- references assumed to be similar end up in the same cluster
- pid -> cluster id

## Produce results
- given a pair (pid), if they belong to the same cluster then duplicated (True)
- otherwise they are distinct (False)
- if pid does not appear in the clustering results, then they are also distinct 


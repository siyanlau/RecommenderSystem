# DSGA1004 - BIG DATA
## Final project

*Handout date*: 2022-04-13

*Submission deadline*: 2022-05-17


# Overview

In the final project, you will apply the tools you have learned in this class to solve a realistic, large-scale applied problem.
Specifically, you will build and evaluate a collaborative-filter based recommender system. 

In either case, you are encouraged to work in **groups of up to 3 students**:

- Groups of 1--2 will need to implement one extension (described below) over the baseline project for full credit.
- Groups of 3 will need to implement two extensions for full credit.

## The data set

In this project, we'll use the [Million Song Dataset](http://millionsongdataset.com/) (MSD) collected by 
> Thierry Bertin-Mahieux, Daniel P.W. Ellis, Brian Whitman, and Paul Lamere. 
> The Million Song Dataset. In Proceedings of the 12th International Society
> for Music Information Retrieval Conference (ISMIR 2011), 2011.

The MSD consists of (you guessed it) one million songs, with metadata (artist, album, year, etc), tags, partial lyrics content, and derived acoustic features.  You need not use all of these aspects of the data, but they are available.
The MSD is hosted in NYU's HPC environment under `/scratch/work/courses/DSGA1004-2021/MSD`.

The user interaction data comes from the [Million Song Dataset Challenge](https://www.kaggle.com/c/msdchallenge)
> McFee, B., Bertin-Mahieux, T., Ellis, D. P., & Lanckriet, G. R. (2012, April).
> The million song dataset challenge. In Proceedings of the 21st International Conference on World Wide Web (pp. 909-916).

The interaction data consists of *implicit feedback*: play count data for approximately one million users.
The interactions have already been partitioned into training, validation, and test sets for you, as described below.

On Peel's HDFS, you will find the following files in `hdfs:/user/bm106/pub/MSD`:

  - `cf_train.parquet`
  - `cf_validation.parquet`
  - `cf_test.parquet`

Each of these files contains tuples of `(user_id, count, track_id)`, indicating how many times (if any) a user listened to a specific track.
For example, the first few rows of `cf_train.parquet` look as follows:

|    | user_id                                  |   count | track_id           |
|---:|:-----------------------------------------|--------:|:-------------------|
|  0 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRIQAUQ128F42435AD |
|  1 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRIRLYL128F42539D1 |
|  2 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       2 | TRMHBXZ128F4238406 |
|  3 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRYQMNI128F147C1C7 |
|  4 | b80344d063b5ccb3212f76538f3d9e43d87dca9e |       1 | TRAHZNE128F9341B86 |

These files are also available under `/scratch/work/public/MillionSongDataset/` if you want to access them from outside of HDFS.


## Basic recommender system [80% of grade]

Your recommendation model should use Spark's alternating least squares (ALS) method to learn latent factor representations for users and items.
Be sure to thoroughly read through the documentation on the [pyspark.ml.recommendation module](https://spark.apache.org/docs/3.0.1/ml-collaborative-filtering.html) before getting started.

This model has some hyper-parameters that you should tune to optimize performance on the validation set, notably: 

  - the *rank* (dimension) of the latent factors, and
  - the regularization parameter.

### Evaluation

Once your model is trained, you will need to evaluate its accuracy on the validation and test data.
Scores for validation and test should both be reported in your final writeup.
Once your model is trained, evaluate it on the test set.
Evaluations should be based on predictions of the top 100 items for each user, and report the ranking metrics provided by spark.
Refer to the [ranking metrics](https://spark.apache.org/docs/3.0.1/mllib-evaluation-metrics.html#ranking-systems) section of the Spark documentation for more details.

The choice of evaluation criteria for hyper-parameter tuning is up to you, as is the range of hyper-parameters you consider, but be sure to document your choices in the final report.
As a general rule, you should explore ranges of each hyper-parameter that are sufficiently large to produce observable differences in your evaluation score.

If you like, you may also use additional software implementations of recommendation or ranking metric evaluations, but be sure to cite any additional software you use in the project.

### Hints

Start small, and get the entire system working start-to-finish before investing time in hyper-parameter tuning!
To avoid over-loading the cluster, I recommend downsampling the training data first to develop a prototype before going to the full dataset.
If you do this, be careful that your downsampled data includes enough users from the validation set to test your model.


### Using the cluster

Please be considerate of your fellow classmates!  The Peel cluster is a limited, shared resource.  Make sure that your code is properly implemented and works efficiently.  If too many people run inefficient code simultaneously, it can slow down the entire cluster for everyone.

Concretely, this means that it will be helpful for you to have a working pipeline that operates on progressively larger sub-samples of the training data.
We suggest building sub-samples of 1%, 5%, and 25% of the data, and then running the entire set of experiments end-to-end on each sample before attempting the entire dataset.
This will help you make efficient progress and debug your implementation, while still allowing other students to use the cluster effectively.
If for any reason you are unable to run on the full dataset, you should report your partial results obtained on the smaller sub-samples.
Any sub-sampling should be performed prior to generating train/validation/test splits.


## Extensions [20% of grade]

For full credit, implement an extension on top of the baseline collaborative filter model.
Again, if you're working in a group of 3, you must implement two extensions for full credit.

The choice of extension is up to you, but here are some ideas:

  - *Comparison to single-machine implementations*: compare Spark's parallel ALS model to a single-machine implementation, e.g. [lightfm](https://github.com/lyst/lightfm) or [lenskit](https://github.com/lenskit/lkpy).  Your comparison should measure both efficiency (model fitting time as a function of data set size) and resulting accuracy.
  - *Fast search*: use a spatial data structure (e.g., LSH or partition trees) to implement accelerated search at query time.  For this, it is best to use an existing library such as [annoy](https://github.com/spotify/annoy), [nmslib](https://github.com/nmslib/nmslib), or [scann](https://github.com/google-research/google-research/tree/master/scann) and you will need to export the model parameters from Spark to work in your chosen environment.  For full credit, you should provide a thorough evaluation of the efficiency gains provided by your spatial data structure over a brute-force search method.
  - *Cold-start*: using supplementary metadata (tags, genres, etc), build a model that can map observable data to the learned latent factor representation for items.  To evaluate its accuracy, simulate a cold-start scenario by holding out a subset of items during training (of the recommender model), and compare its performance to a full collaborative filter model.  *Hint:* you may want to use dask for this.
  - *Exploration*: use the learned representation to develop a visualization of the items and users, e.g., using T-SNE or UMAP.  The visualization should somehow integrate additional information (features, metadata, or genre tags) to illustrate how items are distributed in the learned space.


## What to turn in

In addition to all of your code, produce a final report (not to exceed 6 pages), describing your implementation, evaluation results, and extensions.
Your report should clearly identify the contributions of each member of your group. 
If any additional software components were required in your project, your choices should be described and well motivated here.  

Include a PDF of your final report through Brightspace, and include a link to your github project.

Any additional software components should be documented with installation instructions.


## Checklist

It will be helpful to commit your work in progress to the repository.  Toward this end, we recommend the following timeline:

- [ ] 2022/04/22: popularity baseline model and evaluation on small subset.
- [ ] 2022/04/29: checkpoint submission with baseline results on both small and large datasets.  Preliminary results for matrix factorization on the small dataset.
- [ ] 2022/05/06: scale up to the large dataset and develop extensions.
- [ ] 2022/05/17: final project submission.  **NO EXTENSIONS PAST THIS DATE.**

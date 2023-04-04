# DSGA1004 - BIG DATA
## Final project

*Handout date*: 2023-04-05

*Submission deadline*: 2023-05-16


# Overview

In the final project, you will apply the tools you have learned in this class to solve a realistic, large-scale applied problem.
Specifically, you will build and evaluate a collaborative-filter based recommender system. 

In either case, you are encouraged to work in **groups of up to 3 students**:

- Groups of 1--2 will need to implement one extension (described below) over the baseline project for full credit.
- Groups of 3 will need to implement two extensions for full credit.

## The data set

In this project, we'll use the [ListenBrainz](https://listenbrainz.org/) dataset, which we have mirrored for you in NYU's HPC environment under `/scratch/work/courses/DSGA1004-2021/listenbrainz`.
This data consists of *implicit feedback* from music listening behavior, spanning several thousand users and tens of millions of songs.
**Note**: this is real data, and may contain 

## Basic recommender system [80% of grade]

1.  As a first step, you will need to partition the rating data into training and validation samples as discussed in lecture.
    I recommend writing a script do this in advance, and saving the partitioned data for future use.
    This will reduce the complexity of your experiment code down the line, and make it easier to generate alternative splits if you want to measure the stability of your implementation.

2.  Before implementing a sophisticated model, you begin with a popularity baseline model as discussed in class.
    This should be simple enough to implement with some basic dataframe computations.
    Evaluate your popularity baseline (see below) before moving on to the enxt step.

3.  Your recommendation model should use Spark's alternating least squares (ALS) method to learn latent factor representations for users and items.
    Be sure to thoroughly read through the documentation on the [pyspark.ml.recommendation module](https://spark.apache.org/docs/3.0.1/ml-collaborative-filtering.html) before getting started.
    This model has some hyper-parameters that you should tune to optimize performance on the validation set, notably: 
      - the *rank* (dimension) of the latent factors, and
      - the regularization parameter.

### Evaluation

Once you are able to make predictions—either from the popularity baseline or the latent factor model—you will need to evaluate accuracy on the validation and test data.
Scores for validation and test should both be reported in your write-up.
Evaluations should be based on predictions of the top 100 items for each user, and report the ranking metrics provided by spark.
Refer to the [ranking metrics](https://spark.apache.org/docs/3.0.1/mllib-evaluation-metrics.html#ranking-systems) section of the Spark documentation for more details.

The choice of evaluation criteria for hyper-parameter tuning is up to you, as is the range of hyper-parameters you consider, but be sure to document your choices in the final report.
As a general rule, you should explore ranges of each hyper-parameter that are sufficiently large to produce observable differences in your evaluation score.

If you like, you may also use additional software implementations of recommendation or ranking metric evaluations, but be sure to cite any additional software you use in the project.


### Using the cluster

Please be considerate of your fellow classmates!
The Dataproc cluster is a limited, shared resource. 
Make sure that your code is properly implemented and works efficiently. 
If too many people run inefficient code simultaneously, it can slow down the entire cluster for everyone.


## Extensions [20% of grade]

For full credit, implement an extension on top of the baseline collaborative filter model.
Again, if you're working in a group of 3, you must implement two extensions for full credit.

The choice of extension is up to you, but here are some ideas:

  - *Comparison to single-machine implementations*: compare Spark's parallel ALS model to a single-machine implementation, e.g. [lightfm](https://github.com/lyst/lightfm) or [lenskit](https://github.com/lenskit/lkpy).  Your comparison should measure both efficiency (model fitting time as a function of data set size) and resulting accuracy.
  - *Fast search*: use a spatial data structure (e.g., LSH or partition trees) to implement accelerated search at query time.  For this, it is best to use an existing library such as [annoy](https://github.com/spotify/annoy), [nmslib](https://github.com/nmslib/nmslib), or [scann](https://github.com/google-research/google-research/tree/master/scann) and you will need to export the model parameters from Spark to work in your chosen environment.  For full credit, you should provide a thorough evaluation of the efficiency gains provided by your spatial data structure over a brute-force search method.
  - *Cold-start*: using supplementary metadata (tags, genres, etc), build a model that can map observable data to the learned latent factor representation for items.  To evaluate its accuracy, simulate a cold-start scenario by holding out a subset of items during training (of the recommender model), and compare its performance to a full collaborative filter model.  *Hint:* you may want to use dask for this.

Other extension ideas are welcome as well, but must be approved in advance by the instructional staff.

## What to turn in

In addition to all of your code, produce a final report (no more than 5 pages), describing your implementation, evaluation results, and extensions.
Your report should clearly identify the contributions of each member of your group. 
If any additional software components were required in your project, your choices should be described and well motivated here.  

Include a PDF of your final report through Brightspace.  Specifically, your final report should include the following details:

- Link to your group's GitHub repository
- Documentation of how your train/validation/test splits were generated
    - Any additional pre-processing of the data that you decide to implement
- Choice of evaluation criteria
- Evaluation of popularity baseline on small and full datasets
- Documentation of latent factor model's hyper-parameters
- Evaluation of latent factor model on small and full datasets
- Documentation of extension(s)

Any additional software components that you use should be cited and documented with installation instructions.

## Timeline

It will be helpful to commit your work in progress to the repository.
Toward this end, we recommend the following timeline with a preliminary submission on 2022-04-29:

- [ ] 2023/04/21: popularity baseline model and evaluation on small subset.
- [ ] **2023/04/28**: checkpoint submission with baseline results on both small and large datasets.  Preliminary results for matrix factorization on the small dataset.
- [ ] 2023/05/05: scale up to the large dataset and develop extensions.
- [ ] 2023/05/16: final project submission.  **NO EXTENSIONS PAST THIS DATE.**

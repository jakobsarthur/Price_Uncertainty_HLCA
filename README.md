# Uncertainty in Hybrid LCA from price variance

This library can be used to map BACI trade flows to the reference products of
ecoinvent activities. It is built around a fork the hybridisatoin package for ecoinvent
3.5 and EXIOBASE [pylcaio](https://github.com/MaximeAgez/pylcaio) by [Maxime Agez](https://github.com/MaximeAgez).
The relevant fork is found [here](https://github.com/OASES-project/pylcaio).

Price data is a crucial component linking physical LCA data with monetary MRIO data. 
Commodity price are subject to various market dynamics and trade relations, leading to high
variance in the pirce of the given commodity. This package links BACI trade flows
ecoinvent activities based on their reference product and geographical resolution/information
to obtain a volume weighted price distribution for the refrence products. 

Three [notebooks](https://github.com/jakobsarthur/Price_Uncertainty_HLCA/tree/main/Price_Uncertainty_HLCA/notebooks) are provided to guide the users in how to use the various functions.

## Installation

### Requirements

- Scipy
- Numpy
- Ray
- pypardiso
- [pylcaio](https://github.com/OASES-project/pylcaio)
- pymrio
- ecospold2matrix
- feather

## Relevant Publications

- [Jakobs et al. 2021 - Price Variance in Hybrid-LCA Leads to Significant Uncertainty in Carbon Footprints](https://www.frontiersin.org/articles/10.3389/frsus.2021.666209/full)

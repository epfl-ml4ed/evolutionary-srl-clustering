# Evolutionary Clustering of Apprentices' Behavior in Online Learning Journals for Vocational Education

Learning journals are increasingly used in vocational education to foster self-regulated learning and reflective learning practices. However, for many apprentices, documenting their working experiences is a difficult task. Thus, providing personalized guidance could improve their learning experience.  In this paper, we profile apprentices' evolving learning behavior in an online learning journal throughout their apprenticeship. We propose a novel multi-step clustering pipeline based on a pedagogical framework. The goal is to integrate different learning dimensions into a combined profile that captures changes in learning patterns over time. Specifically, the profiles are described in terms of help-seeking, consistency, regularity, effort, and quality of the activities performed in the learning journal. Our results on two populations of chef apprentices interacting with an online learning journal demonstrate that the proposed pipeline yields interpretable profiles that can be related to academic performance. The obtained profiles can be used as a basis for personalized interventions, with the ultimate goal of improving the apprentices' learning experience.

## File organization
```
├── data
├── docs
│   ├── paper.pdf # paper submission
│   └── formal_notation.pdf # feature creation formulas
│
├── notebooks
├── sql
│   ├── semantic
│   ├── features
│   ├── cohort
│   └── results
└── src
    ├── etl
    ├── features
    ├── models
    └── visualization
```

## Contributing 

This code is provided for educational purposes and aims to facilitate reproduction of our results, and further research in this direction. We have done our best to document, refactor, and test the code before publication.

If you find any bugs or would like to contribute new models, training protocols, etc, please let us know.

Please feel free to file issues and pull requests on the repo and we will address them as we can.


## Citations
If you find this code useful in your work, please cite our paper:
```setup
@ARTICLE{9847370,
  author={Mejia-Domenzain, Paola and Marras, Mirko and Giang, Christian and Cattaneo, Alberto and Käser, Tanja},
  journal={IEEE Transactions on Learning Technologies}, 
  title={Evolutionary Clustering of Apprentices' Self- Regulated Learning Behavior in Learning Journals}, 
  year={2022},
  volume={15},
  number={5},
  pages={579-593},
  doi={10.1109/TLT.2022.3195881}}
```


## License
This code is free software: you can redistribute it and/or modify it under the terms of the GNU General Public License as published by the Free Software Foundation, either version 3 of the License, or (at your option) any later version.

This software is distributed in the hope that it will be useful, but without any warranty; without even the implied warranty of merchantability or fitness for a particular purpose. See the GNU General Public License for details.

You should have received a copy of the GNU General Public License along with this source code. If not, go the following link: http://www.gnu.org/licenses/.

from typing import Dict, List, Set 
from dataclasses import dataclass 


import numpy as np 


class Graph  :
    def __init__(self):
        self.merchant_graph: Dict[np.intc, Set[np.intc]] = {}
        self.pincode_graph: Dict[np.intc, Set[np.intc]] = {}
    
    def find_merchants(self ,pincode : int ) -> Set[np.intc]:
        return self.pincode_graph.get(pincode)

    def find_pincode(self , m_id : int ) -> Set[np.intc]:
        return self.merchant_graph.get(m_id) 




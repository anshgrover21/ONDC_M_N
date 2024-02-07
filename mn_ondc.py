from collections import defaultdict
from typing import Dict, List, Set 
from dataclasses import dataclass 
import dill 
import pickle 
import os 




class Graph  :
    def __init__(self):
        self.merchant_graph: Dict[int, Set[int]] = {}
        self.pincode_graph: Dict[int, Set[int]] = {}
        for i in range ( 0 , 10000000) : 
            self.merchant_graph[i] = set()
            if ( i < 30000):
                self.pincode_graph[i] = set()
    
    def find_merchants(self ,pincode : int ) -> Set[int]:
        return self.pincode_graph.get(pincode)

    def find_pincode(self , m_id : int ) -> Set[int]:
        return self.merchant_graph.get(m_id) 

    # def create_graph(self , merchants_list : Dict[int , Set[int]]) -> None:

    #     for m_id, pin_list in merchants_list.items():
    #         for pin in pin_list:
    #             # set_of_all_pincodes.add(pin)
    #             # print (m_id , pin  , sep = " ") 
    #             self.pincode_graph[pin].add(m_id)
    #         # print (pin_list[0] , self.find_pincode(pin_list[0]) , sep = " ")
    #         self.merchant_graph[m_id].update(pin_list)



if __name__ == "__main__" : 
    graph = Graph ()
    save_path  = os.path.join("data/","data.pkl")
    with open(save_path,"wb") as file_obj:
            dill.dump(graph , file_obj )



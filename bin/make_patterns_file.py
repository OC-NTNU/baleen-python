#!/usr/bin/env python

"""
generate patterns by instantiating patterns with verbs/nouns
saving to a file with Tsurgeon transformations
"""

# TODO 2: needs to be integrated in a better way, e.g. with CL interface


transform_template = """
%==============================================================================
$ {name} $
%==============================================================================

{pattern}
{operation}
"""


                    

#-----------------------------------------------------------------------------    
# GENERAL PATTERNS
#-----------------------------------------------------------------------------  

verb_patterns = { 
    "SUBJ"  : "NP > (S <+(S|VP) (VP <<# {} !< NP))",
    "OBJ"   : "NP > (VP <<# {} )",
    "ATTR1" : "NP < (VBN|VBD|VBG=d1 < {}) !<: =d1 !$. PP",
    "ATTR2" : "NP < (NP < (VBN|VBD|VBG=d1 < {}) $. PP)" 
}


noun_patterns = {
    "NOM1" : "NP <- (/NN/=d1 < {} $ /NN/ )  !$. PP",
    "NOM2" : "NP < (NP <- (/NN/=d1 < {} $ /NN/ )  $. PP)"
}


pcomp_pattern = "NP > (PP <<# ({}) $, (NP <<# {}))"



def generate_patterns(verbs, verb_patterns, nouns,
                      noun_patterns, pcomp_pattern):

    change_verb_patterns = [ (name + "_" + verb, tree_pat.format(verb))
                             for name, tree_pat in verb_patterns.items()
                             for verb in verbs ]

    change_nominal_patterns = [ (name + "_" + noun, tree_pat.format(noun))
                                for name, tree_pat in noun_patterns.items()
                                for noun, _ in nouns ]

    change_pcomp_patterns = [ ("PCOMP_" + noun, 
                               pcomp_pattern.format(prep, noun)) 
                              for noun, prep in nouns]

    return ( change_verb_patterns + 
             change_nominal_patterns +
             change_pcomp_patterns )


def write_patterns_file(filename, patterns):
    with open(filename, "w") as f:
        for name, pattern in patterns:
            operation = "\ndelete d1\n" if "=d1" in pattern else ""
            f.write(transform_template.format(
                name=name, 
                pattern=pattern, 
                operation=operation))



#-----------------------------------------------------------------------------    
# CHANGE
#-----------------------------------------------------------------------------  


change_verbs = (
    "adapt",
    "adjust",
    "alter",
    "change",
    "evolve",
    "fluctuate",
    "modify",
    "modulate",
    "mutate",
    "shift",
    "transform",
    "vary"
)

change_nouns = (
    ("adaptation", "of" ), 
    ("adjustment", "in|of" ), 
    ("alteration", "in|of|to" ), 
    ("change", "in|of|to" ), 
    ("evolution", "in|of" ), 
    ("fluctuation", "in|of" ), 
    ("modification", "in|of|to" ), 
    ("modulation", "in|of" ), 
    ("mutation", "in|of" ), 
    ("shift", "in|of" ), 
    ("transformation", "of" ), 
    ("variation", "in|of"),
    ("variance", "in|of"),
    ("variability", "in|of")
)


change_patterns = generate_patterns(change_verbs, verb_patterns,
                                    change_nouns, noun_patterns, 
                                    pcomp_pattern)


write_patterns_file("change.tfm", change_patterns)
   
    
#-----------------------------------------------------------------------------    
# INCREASE
#-----------------------------------------------------------------------------    
        
increase_verbs = (
    "add",
    "accumulate",
    "augment",
    "boost",
    "double",
    "elevate",
    "enhance",
    "enlarge",
    "expand",
    "gain",
    "grow",
    "heighten",
    "increase",
    "intensify",
    "lengthen",
    "prolong",
    "raise",
    "rise",
    "triple",
    "strengthen"
)

# Rejected because of low precison:
# * advance
# * amplify
# * build
# * deepen
# * develop
# * extend
# * gather
# * import
# * increment
# * inflate
# * multiply
# * magnify
# * progress
# * reinforce
# * suplement
# * swell
# * widen


# * build up


increase_nouns = (
    ("addition", "of"),
    ("accumulation", "of"),
    ("augmentation", "of"),
    ("boost", "in|of"),
    ("double", "in"),
    ("enhancement", "in|of"),
    ("enlargement", "of"),
    ("expansion", "in|of"),
    ("gain", "in|of"),
    ("growth", "of"),
    ("increase", "in|of"),
    ("prolongation", "of"),
    ("raise", "in"),
    ("rise", "in|of")
)

# TODO 3: other increase nouns?


increase_patterns = generate_patterns(increase_verbs, verb_patterns,
                                      increase_nouns, noun_patterns, 
                                      pcomp_pattern)

write_patterns_file("increase.tfm", increase_patterns)


#-----------------------------------------------------------------------------    
# DECREASE
#-----------------------------------------------------------------------------    

decrease_verbs = (
    "cut",
    "decrease",
    "curb",
    "decline",
    "deplete",
    "diminish",
    "drop",
    "exhaust",
    "export",
    "fall",
    "lessen",
    "limit",
    "lower",
    "minimize",
    "mitigate",
    "recede",
    "reduce",
    "shrink"
)
    
decrease_nouns = (
    ("decrease", "in|of"),
    ("decline", "in|of"),
    ("depletion", "of"),
    ("drop", "in|of"),
    ("exhaustion", "of"),
    ("export", "of"),
    ("fall", "in|of"),
    ("limitation", "of"),
    ("loss", "of"),
    ("mitigation", "of"),
    ("reduction", "in|of"),
    ("shrinkage", "in|of")
)    

decrease_patterns = generate_patterns(decrease_verbs, verb_patterns,
                                      decrease_nouns, noun_patterns, 
                                      pcomp_pattern)

write_patterns_file("decrease.tfm", decrease_patterns)
    
    
# TODO 3: comparative adjectives/adverbs
# more - less/fewer
# greater/larger/bigger/wider/broader/taller/wider - smaller/narrower/shorter
# longer - shorter/briefer
# higher - lower
# better - worse
# earlier/sooner - later
# drier - wetter
# warmer/hotter - cooler
# faster - slower
# longer - shorter
# further - closer
# deeper - shallower
# stronger - weaker
# older - younger
# richer - poorer
# heavier - lighter
# harder - softer
# thicker - thinner
# brighter - dimmer
# lighter - darker
# easier - harder
# younger - older/elder
# newer - older
# smoother - rougher
# thicker - leaner
# softer - firmer
# tigther - looser
# fatter - leaner







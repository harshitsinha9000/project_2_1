from pig_util import outputSchems @('value:chararray')
def compute_bpl(bpl,bpl_obj,state):
	if(bpl/bpl_obj>=0.80):
		return 'state';
	else
		return " ";		


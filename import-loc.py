import xml.parsers.expat as expat


class Emitter(object):
    def __init__(self):
        self.recordcount = 0
        self.fieldcount = 0


    def start_element(self, name, attrs):
        if self

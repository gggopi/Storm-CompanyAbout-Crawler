import storm
class SplitSentenceBolt(storm.BasicBolt):
    def process(self,tup):
        if tup.values:
            words = tup.values[0]
            if words:
                storm.emit([words])
        else:
            pass
SplitSentenceBolt().run()
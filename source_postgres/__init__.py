name = "source_postgres"

class SourcePostgres(object):
    dummy_data = [(10000001, "AAAAAAAAAA"),
                  (10000002, "AAAAAAAAAB"),
                  (10000003, "AAAAAAAAAC"),
                  (10000004, "AAAAAAAAAD"),
                  (10000005, "AAAAAAAAAE"),
                  (10000006, "AAAAAAAAAF"),
                  (10000007, "AAAAAAAAAG"),
                  (10000008, "AAAAAAAAAH"),
                  (10000009, "AAAAAAAAAI")
                  ]

    batchsize = 2

    def __init__(self, credentials, source_config):
        self.credentials = credentials
        self.source_config = source_config
        self.curr_batch = 0

    def get_batch(self):
        self.curr_batch += 1
        start_idx = (self.curr_batch - 1) * self.batchsize
        end_idx = self.curr_batch * self.batchsize
        return self.dummy_data[start_idx:end_idx]

    def cleanup(self):
        pass

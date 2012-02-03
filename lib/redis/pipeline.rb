class Redis
  class Pipeline
    attr :futures

    def initialize
      @without_reconnect = false
      @shutdown = false
      @futures = []
    end

    def without_reconnect?
      @without_reconnect
    end

    def shutdown?
      @shutdown
    end

    def call(command, &block)
      # A pipeline that contains a shutdown should not raise ECONNRESET when
      # the connection is gone.
      @shutdown = true if command.first == :shutdown
      future = Future.new(command, block)
      @futures << future
      future
    end

    def call_pipeline(pipeline)
      @shutdown = true if pipeline.shutdown?
      @futures.concat(pipeline.futures)
      nil
    end

    def commands
      @futures.map { |f| f._command }
    end

    def without_reconnect(&block)
      @without_reconnect = true
      yield
    end

    def process_replies(replies)
      error = nil

      # The first error reply will be raised after futures have been set.
      result = replies.each_with_index.map do |reply, i|
        obj = futures[i]._set(reply)
        error = obj if error.nil? && obj.kind_of?(::Exception)
        obj
      end

      raise error if error

      result
    end

    def process_exec_replies(replies)
      error = nil

      exec_replies = replies.last
      exec_replies_index = 0

      # The first error reply will be raised after futures have been set.
      replies.each_with_index.map do |reply, i|
        future = futures[i]
        obj = nil

        if reply == "QUEUED"
          if exec_replies.nil? || exec_replies_index >= exec_replies.length
            # Skip future. Calling #value results in FutureNotReady.
            next
          else
            obj = future._set(exec_replies[exec_replies_index])
            exec_replies[exec_replies_index] = obj
            exec_replies_index += 1
          end
        else
          obj = future._set(reply)
        end

        error = obj if error.nil? && obj.kind_of?(::Exception)
        obj
      end

      raise error if error

      exec_replies
    end
  end

  class FutureNotReady < RuntimeError
    def initialize
      super("Value will be available once the pipeline executes.")
    end
  end

  class Future < BasicObject
    FutureNotReady = ::Redis::FutureNotReady.new

    def initialize(command, transformation)
      @command = command
      @transformation = transformation
      @object = FutureNotReady
    end

    def inspect
      "<Redis::Future #{@command.inspect}>"
    end

    def _set(object)
      @object = @transformation ? @transformation.call(object) : object
    end

    def _command
      @command
    end

    def value
      ::Kernel.raise(@object) if @object.kind_of?(::Exception)
      @object
    end
  end
end

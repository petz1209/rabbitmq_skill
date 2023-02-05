# rabbitmq_skill

<p>A simple messenger app that uses rabbitmq to send messeges between differnt apps</p>

## basic idea
#### messenger 1:
  <p>produces messages messages to queue 1 and consumes from queue 2</p>
  
#### messenger 2:
  <p>produces messages messages to queue 2 and consumes from queue 1</p>


## Guide:
<ol>
<li> start rabbitMQ by running docker-compose up command</li>
<li> start messenger1.main, messenger2.main and message_counter.counter</li>
<li>write new messages in messenger1</li>
</ol>
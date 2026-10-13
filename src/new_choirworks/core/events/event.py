from a2a.types.a2a_pb2 import Message, TaskArtifactUpdateEvent, TaskStatusUpdateEvent

A2AEvent = TaskStatusUpdateEvent | TaskArtifactUpdateEvent | Message

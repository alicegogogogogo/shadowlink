export class ShadowlinkError extends Error {
  constructor(code, status, message) {
    super(message);
    this.name = "ShadowlinkError";
    this.code = code;
    this.status = status;
  }
}

export class ValidationError extends ShadowlinkError {
  constructor(message) {
    super("validation_error", 400, message);
  }
}

export class NotFoundError extends ShadowlinkError {
  constructor(message) {
    super("not_found", 404, message);
  }
}

export class ConflictError extends ShadowlinkError {
  constructor(message) {
    super("conflict", 409, message);
  }
}

/** Raised on the MQTT transport; the broker closes the connection. */
export class ProtocolError extends ShadowlinkError {
  constructor(message, returnCode = null) {
    super("protocol_error", 400, message);
    this.returnCode = returnCode;
  }
}

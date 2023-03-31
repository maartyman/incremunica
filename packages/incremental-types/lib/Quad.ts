import { Quad as n3Quad } from 'n3';

export class Quad extends n3Quad {
  /**
   * An extra attribute that defines if a certain Quad is an addition or deletion.
   * diff = true => The quad is an addition.
   * diff = false => The quad is a deletion.
   */
  public diff?: boolean;
}
